# Use slim Python
FROM python:3.11-slim

WORKDIR /app

# System deps (build + openpyxl needs libjpeg/zlib sometimes)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy and install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . /app

# FastAPI will listen on $PORT (Render sets it)
ENV PORT=10000
EXPOSE 10000

# Start command (Render ignores CMD if you set Start Command in dashboard; either is fine)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]  where do i put this 
