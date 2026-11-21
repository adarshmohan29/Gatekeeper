# One image for every Python service; Compose/Kubernetes pick the entrypoint.
FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends nftables \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
COPY shared shared
COPY services services
RUN pip install --no-cache-dir .
# Trained models (a few KB) are baked in; Compose mounts ../../models over them for dev.
COPY models models
