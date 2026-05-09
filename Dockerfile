FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# System deps:
#  - build-essential, libfreetype6-dev, libpng-dev: needed by Pillow / matplotlib
#  - chromium + chromium-driver: required by FlareSolverr (headless Chrome to bypass Cloudflare)
#  - xvfb: virtual display server FlareSolverr uses when running headed mode
#  - supervisor: process manager so we can run gunicorn AND FlareSolverr in one container
#  - git: needed to clone FlareSolverr
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates git \
    libfreetype6-dev libpng-dev fonts-dejavu-core \
    chromium chromium-driver \
    xvfb \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# ── FlareSolverr setup ───────────────────────────────────────────────────────
# Self-hosted Cloudflare bypass proxy. Listens on 127.0.0.1:8191 (loopback only,
# never exposed to the public internet). Our Python code POSTs Sofascore URLs
# to it and gets back the response after FlareSolverr solved any JS challenge
# using a real Chromium browser.
ENV FLARESOLVERR_HOME=/opt/flaresolverr
RUN git clone --depth 1 https://github.com/FlareSolverr/FlareSolverr.git ${FLARESOLVERR_HOME} \
    && pip install --no-cache-dir -r ${FLARESOLVERR_HOME}/requirements.txt
ENV PORT_FLARESOLVERR=8191 \
    LOG_LEVEL=info \
    LOG_HTML=false \
    CAPTCHA_SOLVER=none \
    BROWSER_TIMEOUT=40000 \
    TEST_URL=https://www.google.com \
    HEADLESS=true \
    PROMETHEUS_ENABLED=false

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

ENV PORT=8080 \
    DB_PATH=/data/tips.db

EXPOSE 8080

# Run gunicorn AND FlareSolverr together via supervisord
CMD ["/usr/bin/supervisord", "-c", "/app/supervisord.conf"]
