use std::collections::HashSet;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::pin::Pin;
use std::println;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use chrono::{FixedOffset, Utc};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use log::{debug, error, info, trace};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{interval, sleep, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const ACCESS_TOKEN_URL: &str = "https://api.bot.qq.com/app/getAppAccessToken";
const GATEWAY_URL: &str = "https://api.bot.qq.com/gateway";
const C2C_MESSAGE_INTENT: i64 = 1 << 25;
const RECONNECT_DELAY: Duration = Duration::from_secs(3);
const DEFAULT_IMAP_HOST: &str = "imap.163.com";
const DEFAULT_IMAP_PORT: u16 = 993;
const USER_OPENID_FILE: &str = "data/user_open_ids.txt";
const PENDING_MESSAGE_FILE: &str = "data/pending_messages.json";
const PENDING_MESSAGE_RETRY_INTERVAL: Duration = Duration::from_secs(60 * 60);
const ACCESS_TOKEN_REFRESH_THRESHOLD: Duration = Duration::from_secs(2 * 60);

#[derive(Debug)]
struct ImapTlsStream {
    inner: tokio_native_tls::TlsStream<tokio::net::TcpStream>,
}

unsafe impl Send for ImapTlsStream {}
impl Unpin for ImapTlsStream {}

impl tokio::io::AsyncRead for ImapTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for ImapTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingMessage {
    user_open_id: String,
    content: String,
    #[serde(default)]
    msg_type: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImapAccount {
    email: String,
    password: String,
    host: String,
    port: u16,
}

#[derive(Debug, Deserialize)]
struct AccessTokenResponse {
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    expires_in: Option<String>,
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GatewayResponse {
    url: String,
}

#[derive(Debug)]
struct AccessTokenError {
    code: Option<i64>,
    message: Option<String>,
    detail: String,
}

impl std::fmt::Display for AccessTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.code, &self.message) {
            (Some(code), Some(message)) => write!(f, "QQ Bot access token failed: code={code}, message={message}. {}", self.detail),
            (Some(code), None) => write!(f, "QQ Bot access token failed: code={code}. {}", self.detail),
            (None, Some(message)) => write!(f, "QQ Bot access token failed: message={message}. {}", self.detail),
            (None, None) => write!(f, "QQ Bot access token failed. {}", self.detail),
        }
    }
}

impl std::error::Error for AccessTokenError {}

#[derive(Debug, Clone)]
pub struct AccessTokenCache {
    pub access_token: String,
    pub expires_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct AccessTokenManager {
    app_id: String,
    client_secret: String,
    token: Arc<tokio::sync::Mutex<String>>,
    expires_at: Arc<tokio::sync::Mutex<SystemTime>>,
}

impl AccessTokenManager {
    async fn new(app_id: String, client_secret: String) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let cache = fetch_access_token_with_expiry(&app_id, &client_secret).await?;

        Ok(Self {
            app_id,
            client_secret,
            token: Arc::new(tokio::sync::Mutex::new(cache.access_token.clone())),
            expires_at: Arc::new(tokio::sync::Mutex::new(cache.expires_at)),
        })
    }

    async fn ensure_valid(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let should_refresh = {
            let expires_at = *self.expires_at.lock().await;
            is_access_token_expiring_soon(expires_at)
        };

        if should_refresh {
            info!("QQ Bot access_token 即将过期或已失效，正在重新获取");
            let cache = fetch_access_token_with_expiry(&self.app_id, &self.client_secret).await?;

            {
                let mut token = self.token.lock().await;
                *token = cache.access_token.clone();
            }

            {
                let mut expires_at = self.expires_at.lock().await;
                *expires_at = cache.expires_at;
            }

            return Ok(cache.access_token);
        }

        Ok(self.token.lock().await.clone())
    }
}

fn is_access_token_expiring_soon(expires_at: SystemTime) -> bool {
    let remaining = match expires_at.duration_since(SystemTime::now()) {
        Ok(remaining) => remaining,
        Err(_) => Duration::from_secs(0),
    };

    remaining <= ACCESS_TOKEN_REFRESH_THRESHOLD
}

async fn fetch_access_token_with_expiry(
    app_id: &str,
    client_secret: &str,
) -> Result<AccessTokenCache, Box<dyn Error + Send + Sync>> {
    let response = Client::new()
        .post(ACCESS_TOKEN_URL)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "appId": app_id,
            "clientSecret": client_secret,
        }))
        .send()
        .await?;

    let body: AccessTokenResponse = response.json().await?;

    if let Some(code) = body.code {
        if code != 0 {
            return Err(Box::new(AccessTokenError {
                code: Some(code),
                message: body.message,
                detail: "Please check appId and clientSecret or retry later.".to_string(),
            }));
        }
    }

    let access_token = match body.access_token {
        Some(token) if !token.trim().is_empty() => token,
        _ => {
            return Err(Box::new(AccessTokenError {
                code: body.code,
                message: body.message,
                detail: "The API response did not include an access_token.".to_string(),
            }));
        }
    };

    let expires_in_seconds = body
        .expires_in
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);

    let expires_at = SystemTime::now() + Duration::from_secs(expires_in_seconds.max(60));

    Ok(AccessTokenCache {
        access_token,
        expires_at,
    })
}

pub async fn get_app_access_token(
    app_id: &str,
    client_secret: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    fetch_access_token_with_expiry(app_id, client_secret)
        .await
        .map(|cache| cache.access_token)
}

pub async fn get_access_token(
    app_id: &str,
    client_secret: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    get_app_access_token(app_id, client_secret).await
}

pub fn persist_user_open_id(user_open_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if user_open_id.trim().is_empty() {
        return Ok(());
    }

    let path = Path::new(USER_OPENID_FILE);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let existing = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(_) => String::new(),
    };

    for line in existing.lines() {
        if line.trim() == user_open_id.trim() {
            return Ok(());
        }
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{user_open_id}")?;
    Ok(())
}

pub fn load_user_open_ids() -> Vec<String> {
    match fs::read_to_string(USER_OPENID_FILE) {
        Ok(content) => content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect(),
        Err(_) => Vec::new(),
    }
}

fn parse_user_open_ids(raw: &str) -> Vec<String> {
    raw.split([';', ',', '\n', '|'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn persist_user_open_ids_from_env() {
    let mut seen = std::collections::HashSet::new();
    let mut queued = Vec::new();

    for key in [
        "USER_OPEN_ID",
        "USER_OPEN_IDS",
        "QQ_USER_OPEN_ID",
        "QQ_USER_OPEN_IDS",
    ] {
        if let Ok(raw) = std::env::var(key) {
            for value in parse_user_open_ids(&raw) {
                if value.trim().is_empty() || !seen.insert(value.clone()) {
                    continue;
                }
                queued.push(value);
            }
        }
    }

    if queued.is_empty() {
        return;
    }

    for value in queued {
        if let Err(err) = persist_user_open_id(&value) {
            error!("启动时写入 user_open_id 失败: {value} err={err}");
        } else {
            info!("启动时写入 user_open_id: {value}");
        }
    }
}

#[derive(Debug, Deserialize)]
struct ApiErrorResponse {
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    message: Option<String>,
}

fn parse_api_error_code(body: &str) -> Option<i64> {
    let payload = serde_json::from_str::<ApiErrorResponse>(body).ok()?;
    payload.code
}

fn is_retryable_message_error(body: &str) -> bool {
    match parse_api_error_code(body) {
        Some(code) => code == 40034100,
        None => body.contains("40034100"),
    }
}

fn save_pending_messages(messages: &[PendingMessage]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = Path::new(PENDING_MESSAGE_FILE);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(messages)?)?;
    Ok(())
}

fn load_pending_messages() -> Vec<PendingMessage> {
    match fs::read_to_string(PENDING_MESSAGE_FILE) {
        Ok(content) => serde_json::from_str::<Vec<PendingMessage>>(&content).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

fn persist_pending_message(
    user_open_id: &str,
    content: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    persist_pending_message_with_type(user_open_id, content, 0)
}

fn persist_pending_message_with_type(
    user_open_id: &str,
    content: &str,
    msg_type: i32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if user_open_id.trim().is_empty() || content.trim().is_empty() {
        return Ok(());
    }

    let mut pending_messages = load_pending_messages();
    if pending_messages.iter().any(|item| {
        item.user_open_id == user_open_id && item.content == content && item.msg_type == msg_type
    }) {
        return Ok(());
    }

    pending_messages.push(PendingMessage {
        user_open_id: user_open_id.to_string(),
        content: content.to_string(),
        msg_type,
    });

    save_pending_messages(&pending_messages)
}

fn delete_pending_message(
    user_open_id: &str,
    content: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    delete_pending_message_with_type(user_open_id, content, 0)
}

fn delete_pending_message_with_type(
    user_open_id: &str,
    content: &str,
    msg_type: i32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut pending_messages = load_pending_messages();
    pending_messages.retain(|item| {
        !(item.user_open_id == user_open_id && item.content == content && item.msg_type == msg_type)
    });
    save_pending_messages(&pending_messages)
}

pub async fn send_message_to_user(
    access_token: &str,
    user_open_id: &str,
    message: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    send_message_to_user_with_type(access_token, user_open_id, message, 0).await
}

pub async fn send_markdown_message_to_user(
    access_token: &str,
    user_open_id: &str,
    markdown: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    send_message_to_user_with_type(access_token, user_open_id, markdown, 2).await
}

async fn send_message_to_user_with_type(
    access_token: &str,
    user_open_id: &str,
    payload_content: &str,
    msg_type: i32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("https://api.bot.qq.com/v2/users/{user_open_id}/messages");
    let request_body = if msg_type == 2 {
        json!({
            "msg_type": 2,
            "markdown": {
                "content": payload_content,
            },
            "msg_seq": 1
        })
    } else {
        json!({
            "msg_type": 0,
            "content": payload_content,
            "msg_seq": 1
        })
    };

    let response = Client::new()
        .post(&url)
        .header("Authorization", format!("QQBot {access_token}"))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?;

    if !response.status().is_success() {
        let body = response.text().await?;
        if is_retryable_message_error(&body) {
            if let Err(err) = persist_pending_message_with_type(user_open_id, payload_content, msg_type) {
                error!("持久化重试消息失败: {err}");
            } else {
                info!("发送消息返回 40034100，已持久化到重试队列: user_open_id={user_open_id}, msg_type={msg_type}");
            }
            return Err(format!("send message failed: {body}").into());
        }
        return Err(format!("send message failed: {body}").into());
    }

    Ok(())
}

async fn retry_pending_messages(access_token_manager: &AccessTokenManager) {
    let pending_messages = load_pending_messages();
    if pending_messages.is_empty() {
        return;
    }

    let access_token = match access_token_manager.ensure_valid().await {
        Ok(token) => token,
        Err(err) => {
            error!("获取有效 access_token 失败，跳过重试消息: {err}");
            return;
        }
    };

    let mut remaining = Vec::new();
    for pending_message in pending_messages {
        let msg_type = pending_message.msg_type;
        match send_message_to_user_with_type(
            &access_token,
            &pending_message.user_open_id,
            &pending_message.content,
            msg_type,
        )
        .await
        {
            Ok(()) => {
                if let Err(err) = delete_pending_message_with_type(
                    &pending_message.user_open_id,
                    &pending_message.content,
                    msg_type,
                ) {
                    error!(
                        "删除已成功重试的持久化消息失败: user_open_id={} content={} err={err}",
                        pending_message.user_open_id, pending_message.content
                    );
                } else {
                    info!(
                        "已重试发送并成功，删除持久化消息: user_open_id={} content={}",
                        pending_message.user_open_id, pending_message.content
                    );
                }
            }
            Err(err) => {
                error!(
                    "重试发送失败: user_open_id={} content={} err={err}",
                    pending_message.user_open_id, pending_message.content
                );
                remaining.push(pending_message);
            }
        }
    }

    if remaining.is_empty() {
        let _ = fs::remove_file(PENDING_MESSAGE_FILE);
        return;
    }

    if let Err(err) = save_pending_messages(&remaining) {
        error!("保存待重试消息失败: {err}");
    }
}

fn start_pending_message_retry_loop(access_token_manager: AccessTokenManager) {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime for pending message retry");
        runtime.block_on(async move {
            loop {
                retry_pending_messages(&access_token_manager).await;
                tokio::time::sleep(PENDING_MESSAGE_RETRY_INTERVAL).await;
            }
        });
    });
}

fn parse_imap_accounts(raw: &str) -> Vec<ImapAccount> {
    let mut accounts = Vec::new();

    for entry in raw.split([';', '\n', ',']) {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }

        let kv = trimmed
            .split_once('|')
            .or_else(|| trimmed.split_once(':'))
            .or_else(|| trimmed.split_once('='));

        let Some((left, password)) = kv else {
            continue;
        };

        let left = left.trim();
        let password = password.trim();
        if left.is_empty() || password.is_empty() {
            continue;
        }

        let mut host = DEFAULT_IMAP_HOST.to_string();
        let mut port = DEFAULT_IMAP_PORT;
        let mut email = left.to_string();

        let parts: Vec<&str> = left.split('#').map(str::trim).filter(|v| !v.is_empty()).collect();
        if parts.len() >= 2 {
            if parts[0].contains('@') {
                email = parts[0].to_string();
            } else if parts.len() >= 3 {
                host = parts[0].to_string();
                if let Ok(parsed_port) = parts[1].parse::<u16>() {
                    port = parsed_port;
                }
                email = parts[2].to_string();
            } else {
                host = parts[0].to_string();
                email = parts[1].to_string();
            }
        }

        if !email.contains('@') {
            continue;
        }

        accounts.push(ImapAccount {
            email: email.to_string(),
            password: password.to_string(),
            host,
            port,
        });
    }

    accounts
}

fn load_imap_accounts() -> Vec<ImapAccount> {
    if let Ok(raw) = std::env::var("IMAP_ACCOUNTS") {
        let accounts = parse_imap_accounts(&raw);
        if !accounts.is_empty() {
            return accounts;
        }
    }

    match (std::env::var("IMAP_EMAIL"), std::env::var("IMAP_PASSWORD")) {
        (Ok(email), Ok(password)) if !email.trim().is_empty() && !password.trim().is_empty() => {
            vec![ImapAccount {
                email,
                password,
                host: DEFAULT_IMAP_HOST.to_string(),
                port: DEFAULT_IMAP_PORT,
            }]
        }
        _ => Vec::new(),
    }
}

fn build_imap_client_identification(email: &str) -> Vec<(&str, Option<&str>)> {
    let support_email = if email.contains('@') {
        email
    } else {
        "support@inkbox.dev"
    };

    vec![
        ("name", Some("notify")),
        ("version", Some(env!("CARGO_PKG_VERSION"))),
        ("vendor", Some("inkbox")),
        ("support-email", Some(support_email)),
    ]
}

fn extract_imap_subject(raw_header: &str) -> Option<String> {
    for line in raw_header.lines() {
        let trimmed = line.trim();
        if trimmed.to_ascii_lowercase().starts_with("subject:") {
            let value = &trimmed[8..].trim();
            return Some(value.to_string());
        }
    }
    None
}

fn extract_imap_date(raw_header: &str) -> Option<String> {
    for line in raw_header.lines() {
        let trimmed = line.trim();
        if trimmed.to_ascii_lowercase().starts_with("date:") {
            let value = &trimmed[5..].trim();
            return Some(value.to_string());
        }
    }
    None
}

fn format_notice_datetime(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "unknown".to_string();
    }

    let utc_plus_8 = chrono::FixedOffset::east_opt(8 * 3600).expect("UTC+8 offset should be valid");

    let parsed = chrono::DateTime::parse_from_rfc2822(trimmed)
        .or_else(|_| chrono::DateTime::parse_from_str(trimmed, "%a, %d %b %Y %H:%M:%S %z"));

    match parsed {
        Ok(dt) => dt.with_timezone(&utc_plus_8).format("%Y-%m-%d %H:%M").to_string(),
        Err(_) => trimmed.to_string(),
    }
}

fn extract_imap_from(raw_header: &str) -> Option<String> {
    for line in raw_header.lines() {
        let trimmed = line.trim();
        if trimmed.to_ascii_lowercase().starts_with("from:") {
            let value = &trimmed[5..].trim();
            return Some(value.to_string());
        }
    }
    None
}

fn extract_imap_to(raw_header: &str) -> Option<String> {
    for line in raw_header.lines() {
        let trimmed = line.trim();
        if trimmed.to_ascii_lowercase().starts_with("to:") {
            let value = &trimmed[3..].trim();
            return Some(value.to_string());
        }
    }
    None
}

fn format_recipient_label(raw_header: &str, fallback_email: &str) -> String {
    let Some(to_value) = extract_imap_to(raw_header) else {
        return fallback_email.to_string();
    };

    let cleaned = to_value.trim();
    if cleaned.is_empty() {
        return fallback_email.to_string();
    }
    cleaned.to_string()
}

fn is_github_sender_header(raw_header: &str) -> bool {
    let Some(from_line) = extract_imap_from(raw_header) else {
        return false;
    };

    let lower = from_line.to_ascii_lowercase();
    lower.contains("@github.com")
        || lower.contains("github.com>")
        || lower.contains("github.com")
}

fn build_imap_header_fetch_queries() -> [&'static str; 3] {
    [
        "BODY.PEEK[HEADER]",
        "BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)]",
        "BODY.PEEK[]",
    ]
}

fn parse_github_notification_subjects(raw_header: &str) -> Vec<String> {
    let mut subjects = Vec::new();
    if !is_github_sender_header(raw_header) {
        return subjects;
    }

    if let Some(subject) = extract_imap_subject(raw_header) {
        if !subject.trim().is_empty() {
            subjects.push(subject);
        }
    }

    subjects
}

async fn mark_seen_flag_for_uid(imap_session: &mut async_imap::Session<ImapTlsStream>, uid: u32) {
    if let Err(err) = imap_session.uid_store(uid.to_string(), "+FLAGS.SILENT (\\Seen)").await {
        error!("标记邮件 UID={uid} 为已读失败: {err}");
    } else {
        info!("已将 GitHub 邮件 UID={uid} 标记为已读");
    }
}

async fn notify_github_email_subjects(
    access_token: &str,
    _source_email: &str,
    recipient_label: &str,
    subject: &str,
    sent_at: Option<&str>,
    uid: u32,
    imap_session: &mut async_imap::Session<ImapTlsStream>,
) -> bool {
    let user_open_ids = load_user_open_ids();
    if user_open_ids.is_empty() {
        info!("未发现持久化的 user_open_id，跳过 GitHub 通知推送");
        return false;
    }

    let received = sent_at
        .map(format_notice_datetime)
        .unwrap_or_else(|| "unknown".to_string());

    let markdown = if sent_at.is_some() {
        format!(
            "# GitHub 通知\n\n**收件人:** {recipient_label}\n**收件时间:** {received}\n**标题:** {subject}"
        )
    } else {
        format!("# GitHub 通知\n\n**收件人:** {recipient_label}\n**标题:** {subject}")
    };

    let mut sent = false;
    for user_open_id in &user_open_ids {
        if let Err(err) = send_markdown_message_to_user(access_token, user_open_id, &markdown).await {
            error!("发送 GitHub 通知到 {user_open_id} 失败: {err}");
        } else {
            sent = true;
            info!("已向 {user_open_id} 发送 GitHub 邮件标题: {subject} (收件人: {recipient_label}, 收件时间: {received})");
        }
    }

    if sent {
        mark_seen_flag_for_uid(imap_session, uid).await;
        return true;
    }

    false
}

async fn poll_imap_github_notifications(access_token: &str, account: &ImapAccount) {
    if account.email.trim().is_empty() || account.password.trim().is_empty() {
        info!("IMAP 邮箱和授权码为空，暂不轮询邮件");
        return;
    }

    let connector = match native_tls::TlsConnector::builder().build() {
        Ok(connector) => tokio_native_tls::TlsConnector::from(connector),
        Err(err) => {
            error!("创建 IMAP TLS 连接器失败: {err}");
            return;
        }
    };

    let tcp = match tokio::net::TcpStream::connect((&account.host[..], account.port)).await {
        Ok(stream) => stream,
        Err(err) => {
            error!("连接 IMAP 服务器失败: host={} port={} err={err}", account.host, account.port);
            return;
        }
    };

    let tls_stream = match connector.connect(&account.host, tcp).await {
        Ok(stream) => stream,
        Err(err) => {
            error!("IMAP TLS 握手失败: host={} port={} err={err}", account.host, account.port);
            return;
        }
    };

    let mut client = async_imap::Client::new(ImapTlsStream { inner: tls_stream });
    if let Err(err) = client.read_response().await {
        error!("读取 IMAP 欢迎信息失败: {err}");
        return;
    }

    let id_values = build_imap_client_identification(&account.email);
    let id_command = format!(
        "ID ({})",
        id_values
            .iter()
            .map(|(key, value)| match value {
                Some(v) => format!("\"{key}\" \"{v}\""),
                None => format!("\"{key}\" NIL"),
            })
            .collect::<Vec<_>>()
            .join(" ")
    );

    if let Err(err) = client.run_command_and_check_ok(&id_command, None).await {
        error!("发送 IMAP ID 失败: {err}");
        return;
    }

    info!("IMAP ID 命令已发送并通过服务端校验");

    let mut imap_session = match client.login(&account.email, &account.password).await {
        Ok(session) => session,
        Err((err, _)) => {
            error!("IMAP 登录失败: email={} host={} port={} err={err}", account.email, account.host, account.port);
            return;
        }
    };

    if let Err(err) = imap_session.select("INBOX").await {
        error!("选择 INBOX 失败: {err}");
        return;
    }

    info!("INBOX 邮箱已连接，开始轮询 GitHub 通知邮件... host={} port={} email={}", account.host, account.port, account.email);

    let unseen = match imap_session.uid_search("UNSEEN").await {
        Ok(ids) => ids,
        Err(err) => {
            error!("搜索未读邮件失败: {err}");
            return;
        }
    };

    info!("unseen 邮件 UID: {:?}", unseen);

    for uid in unseen {
        let header_queries = build_imap_header_fetch_queries();
        let mut header_found = false;
        let mut matched_subject: Option<String> = None;
        let mut matched_date: Option<String> = None;
        let mut recipient_label = format_recipient_label(&format!("To: {}", account.email), &account.email);
        for query in header_queries {
            let fetches = match imap_session.uid_fetch(uid.to_string(), query).await {
                Ok(fetches) => fetches,
                Err(err) => {
                    debug!("UID fetch query {query} 失败: {err}");
                    continue;
                }
            };

            let messages: Vec<_> = match fetches.try_collect().await {
                Ok(messages) => messages,
                Err(err) => {
                    debug!("UID fetch query {query} 读取失败: {err}");
                    continue;
                }
            };

            if messages.is_empty() {
                continue;
            }

            header_found = true;
            for fetch in messages {
                let body = match fetch.body() {
                    Some(body) => body,
                    None => {
                        debug!("uid={} query={} 返回空邮件体，跳过", uid, query);
                        continue;
                    }
                };

                let raw = match std::str::from_utf8(body) {
                    Ok(raw) => raw,
                    Err(err) => {
                        error!("uid={} query={} 邮件体不是 UTF-8: {err}", uid, query);
                        continue;
                    }
                };

                if raw.trim().is_empty() {
                    debug!("uid={} query={} 获取到空邮件内容", uid, query);
                    continue;
                }

                trace!("uid={} query={} body sample: {}", uid, query, raw.lines().take(12).collect::<Vec<_>>().join("\\n"));
                if matched_date.is_none() {
                    matched_date = extract_imap_date(raw);
                }
                if matched_subject.is_none() {
                    matched_subject = parse_github_notification_subjects(raw).into_iter().next();
                }
                let to_label = format_recipient_label(raw, &account.email);
                if to_label != account.email || recipient_label == account.email {
                    recipient_label = to_label;
                }
            }
        }

        if !header_found {
            info!("uid={} 未能读取到任何可用邮件头", uid);
            continue;
        }

        if let Some(subject) = matched_subject {
            let _sent = notify_github_email_subjects(
                access_token,
                &account.email,
                &recipient_label,
                &subject,
                matched_date.as_deref(),
                uid,
                &mut imap_session,
            )
            .await;
        }
    }

    if let Err(err) = imap_session.logout().await {
        error!("退出 IMAP 连接失败: {err}");
    }
}

async fn run_imap_account_monitor(access_token_manager: &AccessTokenManager, accounts: Vec<ImapAccount>) {
    loop {
        let access_token = match access_token_manager.ensure_valid().await {
            Ok(token) => token,
            Err(err) => {
                error!("获取有效 access_token 失败，跳过本轮 IMAP 轮询: {err}");
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
        };

        let mut tasks = Vec::new();
        for account in accounts.iter().cloned() {
            let access_token = access_token.clone();
            let account_clone = account.clone();
            tasks.push(tokio::spawn(async move {
                poll_imap_github_notifications(&access_token, &account_clone).await;
            }));
        }

        for task in tasks {
            if let Err(err) = task.await {
                error!("IMAP 邮件轮询任务执行失败: {err}");
            }
        }

        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}

pub async fn reply_message_to_user(
    access_token: &str,
    user_open_id: &str,
    message: &str,
    msg_id: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("https://api.bot.qq.com/v2/users/{user_open_id}/messages");
    let response = Client::new()
        .post(&url)
        .header("Authorization", format!("QQBot {access_token}"))
        .header("Content-Type", "application/json")
        .json(&json!({
            "msg_type": 0,
            "content": message,
            "msg_id": msg_id,
            "msg_seq": 1
        }))
        .send()
        .await?;

    if !response.status().is_success() {
        let body = response.text().await?;
        return Err(format!("reply message failed: {body}").into());
    }

    Ok(())
}

pub async fn get_gateway_address(access_token: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let response = Client::new()
        .get(GATEWAY_URL)
        .header("Authorization", format!("QQBot {access_token}"))
        .send()
        .await?;

    let body: GatewayResponse = response.json().await?;
    Ok(body.url)
}

pub async fn subscribe_c2c_message_create(
    access_token_manager: &AccessTokenManager,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut session_id: Option<String> = None;
    let mut last_seq: Option<i64> = None;
    let mut heartbeat_interval = 45_000u64;

    loop {
        let access_token = access_token_manager.ensure_valid().await?;
        info!("尝试连接 QQ WebSocket 网关...");
        let gateway_url = get_gateway_address(&access_token).await?;
        let (ws_stream, _) = connect_async(&gateway_url).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut heartbeat = interval(Duration::from_millis(heartbeat_interval));
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut should_reconnect = false;

        loop {
            tokio::select! {
                maybe_message = read.next() => {
                    let Some(message) = maybe_message else {
                        should_reconnect = true;
                        break;
                    };

                    let message = message?;
                    if !message.is_text() && !message.is_binary() {
                        continue;
                    }

                    let text = message.to_text().unwrap_or_default();
                    if text.is_empty() {
                        continue;
                    }

                    let payload: Value = serde_json::from_str(text)?;
                    let op = payload.get("op").and_then(Value::as_i64).unwrap_or(-1);
                    let event_name = payload.get("t").and_then(Value::as_str).unwrap_or("");

                    if let Some(seq) = payload.get("s").and_then(Value::as_i64) {
                        last_seq = Some(seq);
                    }

                    match op {
                        10 => {
                            if let Some(interval_value) = payload["d"]["heartbeat_interval"].as_u64() {
                                heartbeat_interval = interval_value;
                                heartbeat = interval(Duration::from_millis(heartbeat_interval));
                                heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
                            }

                            let current_token = access_token_manager.ensure_valid().await?;
                            let identify = if let Some(session) = &session_id {
                                json!({
                                    "op": 6,
                                    "d": {
                                        "token": format!("QQBot {current_token}"),
                                        "session_id": session,
                                        "seq": last_seq
                                    }
                                })
                            } else {
                                json!({
                                    "op": 2,
                                    "d": {
                                        "token": format!("QQBot {current_token}"),
                                        "intents": C2C_MESSAGE_INTENT,
                                        "shard": [0, 1],
                                        "properties": {
                                            "$os": "linux",
                                            "$browser": "notify",
                                            "$device": "notify"
                                        }
                                    }
                                })
                            };

                            write.send(Message::Text(identify.to_string())).await?;
                        }
                        0 => {
                            debug!("收到事件: {event_name} payload={text}");
                            if event_name == "READY" {
                                if let Some(session) = payload.get("d").and_then(|d| d.get("session_id")).and_then(Value::as_str) {
                                    session_id = Some(session.to_string());
                                }
                                info!("WebSocket READY，已成功鉴权");
                            }

                            if event_name == "RESUMED" {
                                info!("WebSocket RESUMED，已恢复会话");
                            }

                            if event_name == "C2C_MESSAGE_CREATE" {
                                if let Some(user_open_id) = payload
                                    .get("d")
                                    .and_then(|d| d.get("author"))
                                    .and_then(|author| author.get("user_openid"))
                                    .and_then(Value::as_str)
                                {
                                    if let Err(err) = persist_user_open_id(user_open_id) {
                                        error!("持久化 user_open_id 失败: {err}");
                                    } else {
                                        info!("已持久化 user_open_id: {user_open_id}");
                                    }

                                    let message = payload
                                        .get("d")
                                        .and_then(|d| d.get("content"))
                                        .and_then(Value::as_str)
                                        .unwrap_or("");

                                    let msg_id = payload
                                        .get("d")
                                        .and_then(|d| d.get("id"))
                                        .and_then(Value::as_str)
                                        .unwrap_or("");

                                    let reply_message = format!("已收到{message}");
                                    if msg_id.is_empty() {
                                        error!("C2C_MESSAGE_CREATE 缺少 d.id，无法被动回复");
                                    } else {
                                        let current_token = access_token_manager.ensure_valid().await?;
                                        if let Err(err) = reply_message_to_user(&current_token, user_open_id, &reply_message, msg_id).await {
                                            error!("回复消息失败: {err}");
                                        } else {
                                            info!("已回消息给用户 {user_open_id}: {reply_message}");
                                        }
                                    }
                                }
                            }

                            if let Some(d) = payload.get("d") {
                                debug!("{}", serde_json::to_string_pretty(d)?);
                            }
                        }
                        7 => {
                            info!("收到 Reconnect，准备重连");
                            should_reconnect = true;
                            break;
                        }
                        9 => {
                            info!("Invalid Session，重新 identify");
                            session_id = None;
                            last_seq = None;
                            should_reconnect = true;
                            break;
                        }
                        11 => trace!("收到 Heartbeat ACK"),
                        _ => {}
                    }
                }
                _ = heartbeat.tick() => {
                    let payload = json!({ "op": 1, "d": last_seq });
                    write.send(Message::Text(payload.to_string())).await?;
                }
            }
        }

        if should_reconnect {
            info!("WebSocket 连接中断，{} 秒后重连...", RECONNECT_DELAY.as_secs());
            sleep(RECONNECT_DELAY).await;
            continue;
        }

        break;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let utc_plus_8 = FixedOffset::east_opt(8 * 3600).expect("UTC+8 offset should be valid");
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(move |buf, record| {
            let now = Utc::now().with_timezone(&utc_plus_8);
            writeln!(
                buf,
                "{} [{}] {}",
                now.format("%Y-%m-%d %H:%M"),
                record.level(),
                record.args()
            )
        })
        .init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let app_id = match std::env::var("QQ_APP_ID") {
        Ok(value) => value,
        Err(_) => {
            error!("请先设置 QQ_APP_ID 环境变量");
            return;
        }
    };

    let client_secret = match std::env::var("QQ_CLIENT_SECRET") {
        Ok(value) => value,
        Err(_) => {
            error!("请先设置 QQ_CLIENT_SECRET 环境变量");
            return;
        }
    };

    let token_manager = match AccessTokenManager::new(
        app_id,
        client_secret,
    )
    .await
    {
        Ok(token_manager) => token_manager,
        Err(err) => {
            error!("获取 access_token 失败: {err}");
            return;
        }
    };

    let imap_accounts = load_imap_accounts();
    if imap_accounts.is_empty() {
        error!("未配置 IMAP 邮箱，至少需要设置 IMAP_EMAIL/IMAP_PASSWORD 或 IMAP_ACCOUNTS");
        return;
    }

    persist_user_open_ids_from_env();

    info!("获取 access_token 成功，开始建立 WebSocket 长连接...");
    let retry_token = token_manager.clone();

    start_pending_message_retry_loop(retry_token);

    let ws_token_manager = token_manager.clone();
    let mut websocket_task = tokio::spawn(async move {
        if let Err(err) = subscribe_c2c_message_create(&ws_token_manager).await {
            error!("WebSocket 订阅任务退出: {err}");
        }
    });

    for account in &imap_accounts {
        info!("已注册 IMAP 邮件监控账户: {}", account.email);
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("收到 Ctrl+C，程序正在退出");
        }
        _ = &mut websocket_task => {
            info!("WebSocket 订阅线程退出");
        }
        _ = run_imap_account_monitor(&token_manager, imap_accounts) => {
            info!("IMAP 监控循环退出");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_message_error_code_is_detected() {
        let body = r#"{"code":40034100,"message":"message not sent"}"#;
        assert!(is_retryable_message_error(body));
        assert!(!is_retryable_message_error(r#"{"code":10001,"message":"other error"}"#));
    }

    #[test]
    fn pending_message_persistence_round_trip() {
        let path = Path::new(PENDING_MESSAGE_FILE);
        let _ = fs::remove_file(path);

        persist_pending_message("user_1", "hello there").unwrap();
        assert_eq!(load_pending_messages().len(), 1);
        assert_eq!(load_pending_messages()[0].user_open_id, "user_1");
        assert_eq!(load_pending_messages()[0].content, "hello there");

        delete_pending_message("user_1", "hello there").unwrap();
        assert!(load_pending_messages().is_empty());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn parse_success_response() {
        let raw = r#"{
            "access_token": "ACCESS_TOKEN",
            "expires_in": "7200"
        }"#;

        let response: AccessTokenResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(response.access_token.as_deref(), Some("ACCESS_TOKEN"));
        assert_eq!(response.expires_in.as_deref(), Some("7200"));
    }

    #[test]
    fn parse_error_response() {
        let raw = r#"{
            "code": 100007,
            "message": "appid invalid"
        }"#;

        let response: AccessTokenResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(response.code, Some(100007));
        assert_eq!(response.message.as_deref(), Some("appid invalid"));
    }

    #[test]
    fn parse_gateway_response() {
        let raw = r#"{
            "url": "wss://api.bot.qq.com/websocket/"
        }"#;

        let response: GatewayResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(response.url, "wss://api.bot.qq.com/websocket/");
    }

    #[test]
    fn access_token_refreshes_when_two_minutes_or_less_remain() {
        let expires_at = std::time::SystemTime::now() + std::time::Duration::from_secs(119);
        assert!(is_access_token_expiring_soon(expires_at));

        let safe_expires_at = std::time::SystemTime::now() + std::time::Duration::from_secs(180);
        assert!(!is_access_token_expiring_soon(safe_expires_at));
    }

    #[test]
    fn parse_imap_date_header() {
        let raw = "Date: Tue, 19 Nov 2024 10:12:13 +0800\nFrom: noreply@github.com";
        assert_eq!(extract_imap_date(raw), Some("Tue, 19 Nov 2024 10:12:13 +0800".to_string()));
    }

    #[test]
    fn format_notice_datetime_uses_compact_utc8_datetime() {
        assert_eq!(format_notice_datetime("Tue, 19 Nov 2024 10:12:13 +0800"), "2024-11-19 10:12");
        assert_eq!(format_notice_datetime("unknown"), "unknown");
    }

    #[test]
    fn parse_multiple_imap_accounts() {
        let raw = "a@163.com|pass1;b@163.com|pass2\n c@163.com : pass3 ";
        let accounts = parse_imap_accounts(raw);
        assert_eq!(accounts.len(), 3);
        assert_eq!(accounts[0].email, "a@163.com");
        assert_eq!(accounts[0].password, "pass1");
        assert_eq!(accounts[1].email, "b@163.com");
        assert_eq!(accounts[1].password, "pass2");
        assert_eq!(accounts[2].email, "c@163.com");
        assert_eq!(accounts[2].password, "pass3");
    }

    #[test]
    fn parse_imap_accounts_supports_host_and_port() {
        let raw = "imap.163.com#993#user1@163.com|pass1; imap.163.com#user2@163.com|pass2; mail.example.com#465#user3@example.com|pass3";
        let accounts = parse_imap_accounts(raw);
        assert_eq!(accounts.len(), 3);
        assert_eq!(accounts[0].host, "imap.163.com");
        assert_eq!(accounts[0].port, 993);
        assert_eq!(accounts[0].email, "user1@163.com");
        assert_eq!(accounts[0].password, "pass1");
        assert_eq!(accounts[1].host, "imap.163.com");
        assert_eq!(accounts[1].port, 993);
        assert_eq!(accounts[1].email, "user2@163.com");
        assert_eq!(accounts[1].password, "pass2");
        assert_eq!(accounts[2].host, "mail.example.com");
        assert_eq!(accounts[2].port, 465);
        assert_eq!(accounts[2].email, "user3@example.com");
        assert_eq!(accounts[2].password, "pass3");
    }

    #[test]
    fn persist_user_open_id_deduplicates() {
        let path = Path::new(USER_OPENID_FILE);

        persist_user_open_id("openid_1").unwrap();
        persist_user_open_id("openid_1").unwrap();
        persist_user_open_id("openid_2").unwrap();

        let content = fs::read_to_string(path).unwrap();
        assert_eq!(content.lines().filter(|line| *line == "openid_1").count(), 1);
        assert_eq!(content.lines().filter(|line| *line == "openid_2").count(), 1);
    }
}
