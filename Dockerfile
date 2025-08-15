# Use slim Python
FROM python:3.11-slim

# ---- Runtime settings (safer, faster) ----
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    MPLCONFIGDIR=/tmp \
    UVICORN_WORKERS=1 \
    UVICORN_LOG_LEVEL=info \
    PORT=10000 \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Kolkata \
    PYTHONPATH=/app \
    # let CMD be re-targetable without editing the Dockerfile
    MODULE_NAME=PIDSight_backend_clean \
    APP_NAME=app

WORKDIR /app

# ---- System deps ----
# - libjpeg/zlib/libpng/freetype: Pillow + matplotlib runtime
# - fonts-dejavu-core: default font for matplotlib
# - tzdata: timezone data (noninteractive)
# - curl, ca-certificates: healthcheck
# - libgomp1: OpenMP runtime for NumPy wheels
# - libpq5: runtime for psycopg (if non-binary wheel is pulled)
# - build-essential: kept only for wheel builds, removed after pip install
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
      libgomp1 \
      libpq5 \
  && rm -rf /var/lib/apt/lists/*

# ---- Python deps (cache-friendly) ----
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 # remove build toolchain if it was only needed for compiling wheels
 && apt-get purge -y build-essential \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# ---- App code ----
COPY . /app

# ---- Create non-root user ----
RUN useradd -m -r -s /usr/sbin/nologin appuser && chown -R appuser:appuser /app
USER appuser

# ---- Expose port for Render ----
EXPOSE 10000

# ---- Healthcheck ----
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:${PORT}/ping" || exit 1

# ---- Start (shell form so env vars expand) ----
# Change MODULE_NAME/APP_NAME via env if your entrypoint module isn’t PIDSight_backend_clean:app
CMD sh -c 'uvicorn ${MODULE_NAME}:${APP_NAME} --host 0.0.0.0 --port ${PORT:-10000} --proxy-headers --forwarded-allow-ips "*" --workers ${UVICORN_WORKERS:-1} --log-level ${UVICORN_LOG_LEVEL:-info}'
