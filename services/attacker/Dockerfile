FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends hping3 apache2-utils \
    && rm -rf /var/lib/apt/lists/*
COPY attack.sh /usr/local/bin/attack
ENTRYPOINT ["attack"]
