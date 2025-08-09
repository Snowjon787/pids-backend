# Use slim Python
FROM python:3.11-slim

# ---- Runtime settings (safer, faster) ----
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLCONFIGDIR=/tmp \
    UVICORN_WORKERS=1 \
    PORT=10000

WORKDIR /app

# ---- System deps ----
# - libjpeg/zlib/libpng/freetype: for Pillow + matplotlib
# - tzdata: correct IST handling
# - curl, ca-certificates: healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libjpeg62-turbo \
    zlib1g \
    libpng16-16 \
    libfreetype6 \
    tzdata \
    curl \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ---- Python deps (cache-friendly) ----
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ---- App code ----
COPY . /app

# ---- Create non-root user ----
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# ---- Expose port for Render ----
EXPOSE 10000

# ---- Healthcheck (Render respects it) ----
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${PORT}/ping || exit 1

# ---- Start (Render overrides CMD if you set one in dashboard) ----
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "${PORT}", "--proxy-headers", "--forwarded-allow-ips", "*", "--workers", "${UVICORN_WORKERS}"]
