# One image for every Python service; compose picks the entrypoint with `command:`.
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml .
COPY shared shared
COPY services services
RUN pip install --no-cache-dir .
