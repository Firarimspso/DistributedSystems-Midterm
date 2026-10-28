FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STORAGE_PATH=/data

WORKDIR /app

COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY storage_node ./storage_node
COPY metadata_service ./metadata_service
COPY gateway ./gateway
COPY web ./web
COPY workers ./workers

RUN useradd --create-home --uid 10001 storage \
    && mkdir -p /data \
    && chown storage:storage /data

USER storage

EXPOSE 8000

CMD ["uvicorn", "storage_node.app:app", "--host", "0.0.0.0", "--port", "8000"]

