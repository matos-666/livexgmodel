FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates \
    libfreetype6-dev libpng-dev fonts-dejavu-core \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

ENV PORT=8080 \
    DB_PATH=/data/tips.db

EXPOSE 8080

CMD ["sh", "-c", "gunicorn server:app --bind 0.0.0.0:${PORT} --worker-class gevent --workers 2 --worker-connections 1000 --timeout 120 --keep-alive 75 --graceful-timeout 30"]
