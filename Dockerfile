# FROM inkbox/rust:1.99.1
FROM ghcr.1ms.run/inkroomtemp/rust_musl_build
COPY . /app
WORKDIR /app
ENV RUST_LOG=debug
RUN cargo build --release && mv target/$(arch)-unknown-linux-musl/release/notify /notify
# CMD ["cargo", "run"]

FROM scratch
COPY --from=0 /etc/ssl /etc/ssl
COPY --from=0 /notify /notify
STOPSIGNAL SIGINT
ENTRYPOINT ["/notify"]