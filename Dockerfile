# Use slim Python
FROM python:3.11-slim

# ---- Runtime settings (safer, faster) ----
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLCONFIGDIR=/tmp \
    UVICORN_WORKERS=1 \
    PORT=10000 \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Kolkata

WORKDIR /app

# ---- System deps ----
# - libjpeg/zlib/libpng/freetype: runtime for Pillow + matplotlib wheels
# - fonts-dejavu-core: default font for matplotlib
# - tzdata: timezone data (noninteractive)
# - curl, ca-certificates: healthcheck
# - build-essential: only if native builds are needed; removed after pip install
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      libjpeg62-turbo \
      zlib1g \
      libpng16-16 \
      libfreetype6 \
      fonts-dejavu-core \
      tzdata \
      curl \
      ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# ---- Python deps (cache-friendly) ----
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 # If build-essential was only needed for compiling wheels, you can safely remove it now:
 && apt-get purge -y build-essential \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# ---- App code ----
COPY . /app

# ---- Create non-root user ----
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# ---- Expose port for Render ----
EXPOSE 10000

# ---- Healthcheck ----
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:${PORT}/ping" || exit 1

# ---- Start (shell form so env vars expand) ----
CMD sh -c 'uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} --proxy-headers --forwarded-allow-ips "*" --workers ${UVICORN_WORKERS:-1}'
