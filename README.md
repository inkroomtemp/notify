# Notify

一个基于 Rust 的 QQ 机器人通知服务，具备两项核心能力：

- 通过 QQ Bot WebSocket 长连接接收和回复 C2C 消息
- 通过IMAP轮询邮件，识别 GitHub 通知邮件，并通过 QQ机器人 通知指定用户


## 功能概览

- 支持主动发送消息和回复消息
- 支持重试消息队列，失败时写入本地 pending 文件重试
- 监控多个邮箱（通过 `IMAP_ACCOUNTS` 配置）

## 环境变量

### 必填

- `QQ_APP_ID`：QQ 机器人 App ID
- `QQ_CLIENT_SECRET`：QQ 机器人 Client Secret

### IMAP 配置

推荐使用多邮箱配置：

```bash
export IMAP_ACCOUNTS='user1@163.com|password1; user2@163.com|password2'
```

默认为163的imap地址，如果需要其他邮箱，可以这样写

```bash
export IMAP_ACCOUNTS='imap.other.com#993#user1@163.com|password1'
```

或者不指定端口，默认使用993

```bash
export IMAP_ACCOUNTS='imap.other.com#user1@163.com|password1'
```


也支持单邮箱：

```bash
export IMAP_EMAIL='user@163.com'
export IMAP_PASSWORD='your_password'
```

### QQ 用户配置

可在启动时直接写入目标用户：

```bash
export USER_OPEN_ID='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
# 或
export USER_OPEN_IDS='id1,id2;id3'
# 或
export QQ_USER_OPEN_ID='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
# 或
export QQ_USER_OPEN_IDS='id1,id2;id3'
```

## 本地运行

### 1. 安装依赖

```bash
cargo build --release
```

### 2. 设置环境变量

```bash
export QQ_APP_ID='your_app_id'
export QQ_CLIENT_SECRET='your_client_secret'
export IMAP_ACCOUNTS='user@163.com|your_password'
export USER_OPEN_ID='your_qq_user_open_id'
```

### 3. 启动程序

```bash
cargo run
```

## Docker 运行

### 构建镜像

```bash
docker build -t notify:latest .
```

### 运行容器

```bash
docker run -d \
  --name notify \
  -e QQ_APP_ID='your_app_id' \
  -e QQ_CLIENT_SECRET='your_client_secret' \
  -e IMAP_ACCOUNTS='user@163.com|your_password' \
  -e USER_OPEN_ID='your_qq_user_open_id' \
  notify:latest
```

注意：最终镜像使用 `scratch` 运行，并复制了系统证书目录，保证 TLS 连接正常。

## 说明

- 程序会自动已读github邮件，确保后续轮询时不会反复处理已发送的 GitHub 通知。
- 如果 QQ 消息发送失败并返回可重试错误，程序会将消息放入重试队列，下一轮定时重试。

## 备注

该项目用于在 163 邮箱中监听 GitHub 通知邮件，并把通知推送到 QQ 用户，适合做一个轻量的自动提醒机器人。
