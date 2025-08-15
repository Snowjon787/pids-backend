# ====================== PIDSight Backend — Updated & Hardened (Consolidated Single File) ======================
from fastapi import (
    FastAPI, Request, Header, HTTPException, Query, Depends, File, UploadFile
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from typing import Iterable, Optional, List, Dict, Any, Tuple, Union

from datetime import datetime, timedelta, time as dtime
import time
import uuid
import os
import io
import json
import re
import hmac
import hashlib as _hashlib
import logging
from collections import deque
from base64 import b64encode, b64decode

import pandas as pd

# --- Headless matplotlib + writable cache BEFORE importing pyplot ---
os.environ.setdefault("MPLCONFIGDIR", "/tmp")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import requests
from requests.adapters import HTTPAdapter, Retry

import pytz

from sqlalchemy import create_engine, text

# ==============================
# Configuration & Environment
# ==============================
IST = pytz.timezone("Asia/Kolkata")

API_KEY = os.getenv("API_KEY_PIDS", "")
if not API_KEY:
    raise RuntimeError("API_KEY_PIDS env var required")

ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]

HMAC_SECRET = os.getenv("HMAC_SECRET", "")
CLIENT_ID = os.getenv("CLIENT_ID", "")
# Support both env var names to avoid mismatch
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET") or os.getenv("TG_WEBHOOK_SECRET", "")
if not TELEGRAM_WEBHOOK_SECRET:
    raise RuntimeError("TELEGRAM_WEBHOOK_SECRET (or TG_WEBHOOK_SECRET) must be set for /webhook verification")

PUBLIC_WEBHOOK_URL = os.getenv("PUBLIC_WEBHOOK_URL", "https://pids-backend.onrender.com/webhook")
MAX_MEDIA_BYTES = int(os.getenv("MAX_MEDIA_BYTES", "5242880"))  # 5 MB default
RATE_LIMIT_STORAGE_URI = os.getenv("RATE_LIMIT_STORAGE_URI", "").strip()
EXCEL_EXPORT_FILE = "PIDS_Log_Export.xlsx"
MAX_CSV_BYTES = int(os.getenv("MAX_CSV_BYTES", "10485760"))  # 10 MB default

def _parse_ids(raw: str) -> set:
    vals: set = set()
    for x in (raw or "").split(","):
        x = x.strip()
        if not x:
            continue
        vals.add(x)
        if x.lstrip("-").isdigit():
            try:
                vals.add(int(x))
            except Exception:
                pass
    return vals

ALLOWED_CHAT_IDS = _parse_ids(os.getenv("ALLOWED_CHAT_IDS", ""))
ALLOWED_USER_IDS = _parse_ids(os.getenv("ALLOWED_USER_IDS", ""))

# ==============================
# Logging (JSON + redaction)
# ==============================
_SECRET_KEYS = {
    "BOT_TOKEN", "API_KEY_PIDS", "TELEGRAM_WEBHOOK_SECRET", "DATABASE_URL",
    "APP_FERNET_KEY", "hmac_secret"
}

def _mask_secrets(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in _SECRET_KEYS and isinstance(v, str):
                out[k] = "***"
            else:
                out[k] = _mask_secrets(v)
        return out
    if isinstance(obj, str):
        obj = re.sub(r'\\b\\d{9,}:[A-Za-z0-9_-]{35,}\\b', '***', obj)
        obj = re.sub(r'\\b[A-Fa-f0-9]{32,}\\b', '***', obj)
        return obj
    if isinstance(obj, list):
        return [_mask_secrets(x) for x in obj]
    return obj

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k in ("request_id", "method", "path", "status", "latency_ms", "client_ip"):
            if hasattr(record, k):
                payload[k] = getattr(record, k)
        if record.args and isinstance(record.args, dict):
            payload.update(record.args)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(_mask_secrets(payload), ensure_ascii=False)

class RedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _mask_secrets(record.msg)
        elif isinstance(record.msg, dict):
            record.msg = _mask_secrets(record.msg)
        return True

logger = logging.getLogger("pids")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(JsonFormatter())
_handler.addFilter(RedactFilter())
logger.handlers = [_handler]
logger.propagate = False

# child loggers inherit handlers
webhook_logger = logging.getLogger("pids.webhook")

# ==============================
# App & Security Middleware
# ==============================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "X-API-KEY", "X-Telegram-Bot-Api-Secret-Token",
                   "X-Client-Id", "X-TS", "X-Nonce", "X-Signature"],
)

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        resp: Response = await call_next(request)
        resp.headers.setdefault("X-Content-Type-Options","nosniff")
        resp.headers.setdefault("X-Frame-Options","DENY")
        resp.headers.setdefault("Referrer-Policy","no-referrer")
        resp.headers.setdefault("Permissions-Policy","geolocation=(), microphone=(), camera=()")
        resp.headers.setdefault("Strict-Transport-Security","max-age=63072000; includeSubDomains; preload")
        return resp

app.add_middleware(SecurityHeadersMiddleware)

# ---- Rate Limiting (default ON) ----
SLOWAPI_ENABLED = os.getenv("ENABLE_RATELIMIT", "1") == "1"
try:
    if SLOWAPI_ENABLED:
        from slowapi import Limiter
        from slowapi.util import get_remote_address
        from slowapi.middleware import SlowAPIMiddleware
        from slowapi.errors import RateLimitExceeded
    else:
        raise ImportError("Rate limiting disabled by flag")
except Exception:
    class Limiter:
        def __init__(self, *a, **k): pass
        def limit(self, *a, **k):
            def deco(f): return f
            return deco
    def get_remote_address(*a, **k): return "0.0.0.0"
    SlowAPIMiddleware = None
    class RateLimitExceeded(Exception): pass

if SLOWAPI_ENABLED:
    limiter = Limiter(
        key_func=get_remote_address,
        storage_uri=RATE_LIMIT_STORAGE_URI or "memory://",
        strategy="fixed-window",
        default_limits=["100/minute", "10/second"],
        headers_enabled=True,
    )
    app.state.limiter = limiter
    if SlowAPIMiddleware is not None:
        app.add_middleware(SlowAPIMiddleware)
    @app.exception_handler(RateLimitExceeded)
    def _rate_limit_handler(request, exc):
        return PlainTextResponse("Too Many Requests", status_code=429)
else:
    limiter = Limiter()

# ---- Access log middleware ----
class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        req_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        start = time.perf_counter()
        client_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or (request.client.host if request.client else "")
        status = 500
        try:
            response = await call_next(request)
            status = getattr(response, "status_code", 500)
            return response
        finally:
            elapsed = (time.perf_counter() - start) * 1000.0
            rec = logging.LogRecord(
                name="pids.access",
                level=logging.INFO,
                pathname=__file__,
                lineno=0,
                msg=f"{request.method} {request.url.path}",
                args=None,
                exc_info=None
            )
            rec.request_id = req_id
            rec.method = request.method
            rec.path = request.url.path
            rec.status = status
            rec.latency_ms = round(elapsed, 2)
            rec.client_ip = client_ip
            logger.handle(rec)

app.add_middleware(RequestContextMiddleware)

# ---- Cache request body for signature verification ----
class CacheBodyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            request._body = await request.body()
        except Exception:
            request._body = b""
        return await call_next(request)

app.add_middleware(CacheBodyMiddleware)

# ==============================
# Database & Schema
# ==============================
raw_url = os.environ.get("DATABASE_URL", "")
if not raw_url:
    raise RuntimeError("DATABASE_URL env var required")

if raw_url.startswith("postgres://"):
    raw_url = raw_url.replace("postgres://", "postgresql+psycopg://", 1)
elif raw_url.startswith("postgresql://") and "+psycopg" not in raw_url:
    raw_url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)

DATABASE_URL = raw_url

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
    pool_recycle=1800,
    pool_timeout=30,
    future=True,
)

def db_exec(sql: str, params: Optional[Dict[str, Any]] = None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

def db_query_all(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        conn.execute(text("SET statement_timeout TO 5000"))
        res = conn.execute(text(sql), params or {})
        cols = res.keys()
        return [dict(zip(cols, row)) for row in res.fetchall()]

def db_query_one(sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    rows = db_query_all(sql, params)
    return rows[0] if rows else None

def init_db_pg():
    ddl = """
    CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS group_mappings (
        id SERIAL PRIMARY KEY,
        start_ch DOUBLE PRECISION,
        end_ch DOUBLE PRECISION,
        group_name TEXT,
        chat_id TEXT,
        time_window TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_group_range ON group_mappings(start_ch, end_ch, time_window);

    CREATE TABLE IF NOT EXISTS linewalkers (
        id SERIAL PRIMARY KEY,
        start_ch DOUBLE PRECISION,
        end_ch DOUBLE PRECISION,
        line_walker TEXT,
        saved_at TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_linewalkers_range ON linewalkers(start_ch, end_ch);

    CREATE TABLE IF NOT EXISTS supervisors (
        id SERIAL PRIMARY KEY,
        start_ch DOUBLE PRECISION,
        end_ch DOUBLE PRECISION,
        supervisor TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_supervisors_range ON supervisors(start_ch, end_ch);

    CREATE TABLE IF NOT EXISTS npv_contacts (
        id SERIAL PRIMARY KEY,
        start_ch DOUBLE PRECISION,
        end_ch DOUBLE PRECISION,
        npv TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_npv_range ON npv_contacts(start_ch, end_ch);

    CREATE TABLE IF NOT EXISTS sent_logs (
        id SERIAL PRIMARY KEY,
        Date TEXT,
        Time TEXT,
        OD DOUBLE PRECISION,
        CH DOUBLE PRECISION,
        Section TEXT,
        Linewalker TEXT,
        Supervisor TEXT,
        NPV TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_sent_logs_time ON sent_logs(Date, Time);
    CREATE INDEX IF NOT EXISTS ix_sent_logs_keys ON sent_logs(Section, Linewalker, Supervisor, NPV);

    CREATE TABLE IF NOT EXISTS received_messages (
        id SERIAL PRIMARY KEY,
        Timestamp TEXT,
        Linewalker TEXT,
        Message TEXT,
        Sender TEXT,
        MediaId INTEGER,
        MediaType TEXT,
        MediaThumb TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_received_time ON received_messages(Timestamp);

    CREATE TABLE IF NOT EXISTS duty_status (
        id SERIAL PRIMARY KEY,
        Timestamp TEXT,
        Linewalker TEXT,
        Duty_on TEXT,
        Duty_off TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_duty_time ON duty_status(Timestamp);

    CREATE TABLE IF NOT EXISTS received_media (
        id SERIAL PRIMARY KEY,
        Timestamp TEXT,
        Linewalker TEXT,
        MediaType TEXT,
        FileId TEXT,
        FileUniqueId TEXT,
        FilePath TEXT,
        Caption TEXT,
        Bytes BYTEA
    );
    CREATE INDEX IF NOT EXISTS ix_media_time ON received_media(Timestamp);

    -- Per-client auth for signed requests
    CREATE TABLE IF NOT EXISTS api_clients (
        client_id TEXT PRIMARY KEY,
        api_key_hash TEXT NOT NULL,
        hmac_secret TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT NOW()
    );

    -- Replay protection storage
    CREATE TABLE IF NOT EXISTS used_nonces (
        client_id TEXT NOT NULL,
        nonce TEXT NOT NULL,
        ts TIMESTAMP NOT NULL,
        PRIMARY KEY (client_id, nonce)
    );
    CREATE INDEX IF NOT EXISTS ix_used_nonces_ts ON used_nonces(ts);
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)

def table_has_column(table: str, column: str) -> bool:
    sql = text("""
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = :t AND column_name = :c 
        LIMIT 1
    """)
    with engine.begin() as conn:
        return conn.execute(sql, {"t": table.lower(), "c": column}).scalar() is not None

def ensure_schema():
    alters = []
    if not table_has_column("received_messages", "mediaid"):
        alters.append("ALTER TABLE received_messages ADD COLUMN IF NOT EXISTS MediaId INTEGER")
    if not table_has_column("received_messages", "mediatype"):
        alters.append("ALTER TABLE received_messages ADD COLUMN IF NOT EXISTS MediaType TEXT")
    if not table_has_column("received_messages", "mediathumb"):
        alters.append("ALTER TABLE received_messages ADD COLUMN IF NOT EXISTS MediaThumb TEXT")
    if not table_has_column("duty_status", "duty_on"):
        alters.append("ALTER TABLE duty_status ADD COLUMN IF NOT EXISTS Duty_on TEXT")
    if not table_has_column("duty_status", "duty_off"):
        alters.append("ALTER TABLE duty_status ADD COLUMN IF NOT EXISTS Duty_off TEXT")
    if alters:
        with engine.begin() as conn:
            for stmt in alters:
                conn.exec_driver_sql(stmt)

init_db_pg()
ensure_schema()

# ==============================
# Encryption (Fernet)
# ==============================
import base64
from cryptography.fernet import Fernet, InvalidToken

def _normalize_fernet_env(var: str) -> str:
    k = (var or "").strip()
    # strip wrapping quotes
    if (k.startswith("'") and k.endswith("'")) or (k.startswith('"') and k.endswith('"')):
        k = k[1:-1]
    # strip accidental b'...'/b"..."
    if (k.startswith("b'") and k.endswith("'")) or (k.startswith('b"') and k.endswith('"')):
        k = k[2:-1]
    # remove whitespace/newlines
    k = re.sub(r"\\s+", "", k)
    # add missing base64 padding
    k += "=" * ((4 - len(k) % 4) % 4)
    return k

APP_FERNET_KEY = os.getenv("APP_FERNET_KEY", "")
key_str = _normalize_fernet_env(APP_FERNET_KEY)

try:
    raw = base64.urlsafe_b64decode(key_str)
    if len(raw) != 32:
        raise ValueError(f"decoded length={len(raw)} (expected 32)")
    FERNET = Fernet(key_str.encode())
except Exception as e:
    # Don’t leak the key; show only diagnostics that help
    raise RuntimeError(
        "APP_FERNET_KEY invalid. It must be a 44-char urlsafe base64 string from Fernet.generate_key()."
        f" Got len={len(APP_FERNET_KEY)} after trimming."
    ) from e

def enc_bytes(b: Optional[bytes]) -> Optional[bytes]:
    if b is None:
        return None
    return FERNET.encrypt(b)

def dec_bytes(b: Optional[bytes]) -> Optional[bytes]:
    if b is None:
        return None
    try:
        return FERNET.decrypt(b)
    except InvalidToken:
        # legacy/plain — return as-is
        return b

def enc_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    return FERNET.encrypt(s.encode()).decode()

def dec_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    if isinstance(s, str) and s.startswith("gAAAA"):
        try:
            return FERNET.decrypt(s.encode()).decode()
        except InvalidToken:
            return s
    return s

# Encrypt-at-rest toggles
STORE_RECEIVED_TEXT = os.getenv("STORE_RECEIVED_TEXT", "1") == "1"   # 0 = store hash only
STORE_MEDIA_BYTES   = os.getenv("STORE_MEDIA_BYTES", "1") == "1"     # 0 = store no bytes

# ==============================
# Sealed / Panic Lock
# ==============================
SEALED_DEFAULT = os.getenv("APP_SEAL_MODE", "0") == "1"  # if 1, service starts locked
_LOCKED = SEALED_DEFAULT

def is_locked() -> bool:
    return _LOCKED

def _set_locked(v: bool):
    global _LOCKED
    _LOCKED = v

def require_unlocked():
    if is_locked():
        raise HTTPException(status_code=423, detail="Service sealed/locked")
    return True

# anomaly tracker: auto-lock on repeated failures
_failed_events = deque(maxlen=50)

def record_fail(tag: str):
    _failed_events.append((tag, time.time()))

def too_many_recent_fails(window_s: int = 60, threshold: int = 5) -> bool:
    cutoff = time.time() - window_s
    return sum(1 for _, ts in _failed_events if ts >= cutoff) >= threshold

# ==============================
# Settings (with BOT_TOKEN encryption)
# ==============================
def get_setting(key: str) -> Optional[str]:
    row = db_query_one("SELECT value FROM app_settings WHERE key = :k", {"k": key})
    v = row["value"] if row else None
    if key == "BOT_TOKEN" and v:
        v = dec_text(v)
    return v

def set_setting(key: str, value: str) -> None:
    if key == "BOT_TOKEN" and value:
        value = enc_text(value)
    db_exec("""
        INSERT INTO app_settings(key, value) 
        VALUES(:k, :v) 
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """, {"k": key, "v": value})

# ==============================
# Auth: API Key + Signed Requests
# ==============================
def verify_api_key(x_api_key: str = Header(..., alias="X-API-KEY")):
    if x_api_key != API_KEY:
        record_fail("api_key")
        if too_many_recent_fails():
            _set_locked(True)  # auto-seal on brute force
        raise HTTPException(status_code=403, detail="Unauthorized")
    return True

def _sha256_hex(s: str) -> str:
    return _hashlib.sha256(s.encode()).hexdigest()

def _client_row(client_id: str) -> Optional[Dict[str, Any]]:
    return db_query_one(
        "SELECT client_id, api_key_hash, hmac_secret, enabled FROM api_clients WHERE client_id=:c",
        {"c": client_id}
    )

def _nonce_seen(client_id: str, nonce: str) -> bool:
    r = db_query_one("SELECT 1 FROM used_nonces WHERE client_id=:c AND nonce=:n", {"c": client_id, "n": nonce})
    return r is not None

def _remember_nonce(client_id: str, nonce: str, ts: datetime):
    db_exec("INSERT INTO used_nonces (client_id, nonce, ts) VALUES (:c,:n,:t) ON CONFLICT DO NOTHING",
            {"c": client_id, "n": nonce, "t": ts.strftime("%Y-%m-%d %H:%M:%S")})

# Signed-request dependency
def verify_signed_request(
    request: Request,
    x_client_id: str = Header(..., alias="X-Client-Id"),
    x_api_key: str  = Header(..., alias="X-API-KEY"),
    x_ts: str       = Header(..., alias="X-TS"),
    x_nonce: str    = Header(..., alias="X-Nonce"),
    x_sig: str      = Header(..., alias="X-Signature"),
):
    if x_api_key != API_KEY:
        record_fail("api_key")
        if too_many_recent_fails():
            _set_locked(True)
        raise HTTPException(status_code=403, detail="Unauthorized")

    row = _client_row(x_client_id)
    if not row or not row.get("enabled"):
        record_fail("client_disabled")
        raise HTTPException(403, "Client disabled or unknown")
    if row["api_key_hash"] != _sha256_hex(x_api_key):
        record_fail("client_key_mismatch")
        raise HTTPException(403, "API key mismatch for client")

    try:
        ts_dt = datetime.utcfromtimestamp(int(x_ts))
    except Exception:
        record_fail("bad_ts")
        raise HTTPException(400, "Bad X-TS")
    now = datetime.utcnow()
    if abs((now - ts_dt).total_seconds()) > 60:
        record_fail("stale")
        raise HTTPException(401, "Stale request")

    if _nonce_seen(x_client_id, x_nonce):
        record_fail("replay")
        raise HTTPException(401, "Replay")
    _remember_nonce(x_client_id, x_nonce, ts_dt)

    body = getattr(request, "_body", b"") or b""
    payload = b"|".join([x_client_id.encode(), x_ts.encode(), x_nonce.encode(), body])
    mac = hmac.new((row["hmac_secret"] or "").encode(), payload, _hashlib.sha256).digest()
    expected = b64encode(mac).decode()

    if not hmac.compare_digest(expected, x_sig):
        record_fail("bad_sig")
        raise HTTPException(401, "Bad signature")
    return True

# ==============================
# Simple health (public)
# ==============================
@app.get("/ping")
def ping():
    return {"status": "ok", "message": "PIDS backend (secured) alive."}

# ---------------- Master CSV cache & interpolation ----------------
_MASTER_CACHE: Dict[str, pd.DataFrame] = {}
_MASTER_MTIME: Optional[float] = None
_MASTER_FILE = "OD_CH_Master.csv"

def _load_master_from_disk() -> Tuple[Dict[str, pd.DataFrame], Optional[float]]:
    section_data: Dict[str, pd.DataFrame] = {}
    mtime = None
    try:
        mtime = os.path.getmtime(_MASTER_FILE)
        df_master = pd.read_csv(_MASTER_FILE)
        df_master = df_master.dropna(subset=["Section", "OD", "CH", "Diff"])
        df_master["OD"] = pd.to_numeric(df_master["OD"], errors="coerce")
        df_master["CH"] = pd.to_numeric(df_master["CH"], errors="coerce")
        df_master["Diff"] = pd.to_numeric(df_master["Diff"], errors="coerce")
        df_master = df_master.dropna(subset=["OD", "CH", "Diff"])
        for section in df_master["Section"].unique():
            df_section = df_master[df_master["Section"] == section].copy()
            df_section = df_section.sort_values("OD").reset_index(drop=True)
            section_data[section] = df_section
    except Exception as e:
        logger.exception({"msg": f"Error loading {_MASTER_FILE}", "error": str(e)})
    return section_data, mtime

def _maybe_reload_master():
    global _MASTER_CACHE, _MASTER_MTIME
    try:
        current_mtime = os.path.getmtime(_MASTER_FILE)
    except Exception:
        current_mtime = None
    if current_mtime is None:
        return
    if _MASTER_MTIME is None or current_mtime != _MASTER_MTIME or not _MASTER_CACHE:
        cache, mtime = _load_master_from_disk()
        if cache:
            _MASTER_CACHE = cache
            _MASTER_MTIME = mtime

def get_section_data() -> Dict[str, pd.DataFrame]:
    _maybe_reload_master()
    return _MASTER_CACHE

def force_reload_master() -> dict:
    global _MASTER_CACHE, _MASTER_MTIME
    _MASTER_CACHE = {}
    _MASTER_MTIME = None
    cache, mtime = _load_master_from_disk()
    loaded = bool(cache)
    if loaded:
        _MASTER_CACHE = cache
        _MASTER_MTIME = mtime
    return {
        "reloaded": loaded,
        "sections": list(cache.keys()) if loaded else [],
        "mtime": datetime.fromtimestamp(mtime, IST).strftime("%Y-%m-%d %H:%M:%S") if mtime else None,
    }

def interpolate_ch(df: pd.DataFrame, od: float) -> List[float]:
    ch_matches: List[float] = []
    od1 = df["OD"].values[:-1]; od2 = df["OD"].values[1:]
    ch1 = df["CH"].values[:-1]; ch2 = df["CH"].values[1:]
    diff = df["Diff"].values[:-1]
    mask = (od1 <= od) & (od <= od2) & (diff != 0)
    if not mask.any():
        return ch_matches
    for i in mask.nonzero()[0]:
        ch = ch1[i] + ((ch2[i] - ch1[i]) * (od - od1[i]) / diff[i])
        ch_matches.append(round(float(ch), 3))
    return ch_matches

def interpolate_od(df: pd.DataFrame, ch: float) -> Optional[int]:
    ch1 = df["CH"].values[:-1]; ch2 = df["CH"].values[1:]
    od1 = df["OD"].values[:-1]; od2 = df["OD"].values[1:]
    mask = (ch1 <= ch) & (ch <= ch2) & ((ch2 - ch1) != 0)
    if not mask.any():
        return None
    i = int(mask.nonzero()[0][0])
    interpolated = od1[i] + ((ch - ch1[i]) * (od2[i] - od1[i])) / (ch2[i] - ch1[i])
    return int(round(float(interpolated)))

# ---------------- Models used in routes ----------------
class TokenUpdateRequest(BaseModel):
    token: str

class GroupMappingItem(BaseModel):
    start_ch: float
    end_ch: float
    group_name: str
    chat_id: Union[str, int]
    time_window: str

class LineWalkerItem(BaseModel):
    start_ch: float
    end_ch: float
    line_walker: str

class SupervisorItem(BaseModel):
    start_ch: float
    end_ch: float
    supervisor: str

class NPVItem(BaseModel):
    start_ch: float
    end_ch: float
    npv: str

class RefreshRequest(BaseModel):
    reload_master: bool = True
    migrate_from_json: bool = False
    reset_webhook: bool = False

class AlertPayload(BaseModel):
    od: float
    ch: float
    section: str
    line_walker: str = ""
    supervisor: Optional[str] = ""
    npv: Optional[str] = ""

# ---------------- Settings & masters endpoints ----------------
@app.post("/upload_master_csv2")
@limiter.limit("10/minute")
def upload_master_csv2(
    request: Request,
    file: UploadFile = File(..., description="CSV with columns: Section, OD, CH, Diff"),
    unsealed: bool = Depends(require_unlocked),
    auth=Depends(verify_api_key),
    sig=Depends(verify_signed_request),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted")
    try:
        raw = file.file.read(MAX_CSV_BYTES + 1)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Read failed: {e}")
    if len(raw) == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(raw) > MAX_CSV_BYTES:
        raise HTTPException(status_code=413, detail=f"CSV too large (>{MAX_CSV_BYTES} bytes)")
    try:
        df = pd.read_csv(io.BytesIO(raw))
        required = {"Section", "OD", "CH", "Diff"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(sorted(missing))}")
        df["OD"] = pd.to_numeric(df["OD"], errors="coerce")
        df["CH"] = pd.to_numeric(df["CH"], errors="coerce")
        df["Diff"] = pd.to_numeric(df["Diff"], errors="coerce")
        if df[["OD","CH","Diff"]].isna().all(axis=None):
            raise HTTPException(status_code=400, detail="OD/CH/Diff columns contain no numeric values.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CSV parse failed: {e}")

    tmp_path = _MASTER_FILE + ".tmp"
    bak_path = _MASTER_FILE + ".bak"
    with open(tmp_path, "wb") as f:
        f.write(raw)
    _ = pd.read_csv(tmp_path)
    if os.path.exists(_MASTER_FILE):
        try:
            if os.path.exists(bak_path):
                os.remove(bak_path)
            os.replace(_MASTER_FILE, bak_path)
        except Exception:
            pass
    os.replace(tmp_path, _MASTER_FILE)
    info = force_reload_master()
    return {
        "status": "uploaded",
        "filename": file.filename,
        "bytes": len(raw),
        "sections_loaded": info.get("sections", []),
        "mtime": info.get("mtime"),
    }

@app.get("/master/info")
def master_info(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    try:
        st = os.stat(_MASTER_FILE)
        _maybe_reload_master()
        return {
            "path": _MASTER_FILE,
            "size_bytes": st.st_size,
            "mtime": datetime.fromtimestamp(st.st_mtime, IST).strftime("%Y-%m-%d %H:%M:%S"),
            "sections": list(_MASTER_CACHE.keys()),
        }
    except FileNotFoundError:
        return {"path": _MASTER_FILE, "exists": False}

# ---------------- Section & conversion utilities ----------------
@app.get("/get_sections")
def get_sections():
    try:
        section_data = get_section_data()
        return list(section_data.keys())
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/calculate_ch_for_section")
def calculate_ch_for_section(section: str = Query(...), od: float = Query(...)):
    section_data = get_section_data()
    df = section_data.get(section)
    if df is None:
        return {"error": f"Section '{section}' not found."}
    ch_matches = interpolate_ch(df, od)
    if not ch_matches:
        return {"error": "OD out of range or no valid interpolation found."}
    return {"ch_matches": ch_matches}

@app.get("/convert/ch-to-od")
def convert_ch_to_od(section: str = Query(...), ch: float = Query(...)):
    section_data = get_section_data()
    df = section_data.get(section)
    if df is None:
        return {"error": f"Section '{section}' not found."}
    od = interpolate_od(df, ch)
    if od is None:
        return {"error": "CH out of range."}
    return {"od": od}

# ---------------- Group mapping & roles ----------------
def load_group_mappings() -> List[Dict[str, Any]]:
    rows = db_query_all("""
        SELECT start_ch, end_ch, group_name, chat_id, time_window 
        FROM group_mappings
    """)
    for r in rows:
        r["chat_id"] = (str(r["chat_id"]).strip() if r["chat_id"] is not None else "")
        r["time_window"] = (r["time_window"] or "").strip().lower()
    return rows

def update_group_mappings(mappings: List[Dict[str, Any]]) -> None:
    normalized: List[Dict[str, Any]] = []
    for m in mappings:
        start_ch = float(m.get("start_ch", 0.0))
        end_ch = float(m.get("end_ch", 0.0))
        group_name = str(m.get("group_name", "")).strip()
        chat_id = str(m.get("chat_id", "")).strip()
        time_window = str(m.get("time_window", "")).strip().lower()
        normalized.append({
            "start_ch": start_ch,
            "end_ch": end_ch,
            "group_name": group_name,
            "chat_id": chat_id,
            "time_window": time_window
        })
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM group_mappings"))
        conn.execute(text("""
            INSERT INTO group_mappings 
            (start_ch, end_ch, group_name, chat_id, time_window)
            VALUES (:start_ch, :end_ch, :group_name, :chat_id, :time_window)
        """), normalized)

def load_linewalkers():
    return db_query_all("SELECT start_ch, end_ch, line_walker FROM linewalkers")

def save_linewalkers(data: List[Dict[str, Any]]):
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    rows = [{
        "start_ch": d["start_ch"], 
        "end_ch": d["end_ch"], 
        "line_walker": d["line_walker"], 
        "saved_at": now
    } for d in data]
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM linewalkers"))
        conn.execute(text("""
            INSERT INTO linewalkers 
            (start_ch, end_ch, line_walker, saved_at)
            VALUES (:start_ch, :end_ch, :line_walker, :saved_at)
        """), rows)

def load_supervisors():
    return db_query_all("SELECT start_ch, end_ch, supervisor FROM supervisors")

def save_supervisors(data: List[Dict[str, Any]]):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM supervisors"))
        conn.execute(text("""
            INSERT INTO supervisors 
            (start_ch, end_ch, supervisor)
            VALUES (:start_ch, :end_ch, :supervisor)
        """), data)

def load_npv():
    return db_query_all("SELECT start_ch, end_ch, npv FROM npv_contacts")

def save_npv(data: List[Dict[str, Any]]):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM npv_contacts"))
        conn.execute(text("""
            INSERT INTO npv_contacts 
            (start_ch, end_ch, npv)
            VALUES (:start_ch, :end_ch, :npv)
        """), data)

def get_linewalker_by_ch(ch: float) -> Optional[str]:
    for entry in load_linewalkers():
        if entry["start_ch"] <= ch <= entry["end_ch"]:
            return entry["line_walker"]
    return None

def get_supervisor_by_ch(ch: float) -> Optional[str]:
    for entry in load_supervisors():
        if entry["start_ch"] <= ch <= entry["end_ch"]:
            return entry["supervisor"]
    return None

def get_npv_by_ch(ch: float) -> Optional[str]:
    for entry in load_npv():
        if entry["start_ch"] <= ch <= entry["end_ch"]:
            return entry["npv"]
    return None

# ---------------- Settings & roles endpoints ----------------
@app.post("/update_token")
@limiter.limit("20/minute")
def update_token(request: Request, data: TokenUpdateRequest,
                 unsealed: bool = Depends(require_unlocked),
                 auth=Depends(verify_api_key), sig=Depends(verify_signed_request)):
    set_setting("BOT_TOKEN", data.token.strip())
    return {"status": "success", "message": "Bot token updated"}

@app.post("/update_group_mappings")
@limiter.limit("20/minute")
def update_group_mappings_endpoint(
    request: Request, 
    payload: List[GroupMappingItem],
    unsealed: bool = Depends(require_unlocked),
    auth=Depends(verify_api_key), sig=Depends(verify_signed_request)
):
    update_group_mappings([item.dict() for item in payload])
    return {"status": "success", "updated_count": len(payload)}

@app.get("/view_linewalkers")
def view_linewalkers(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    return load_linewalkers()

@app.post("/edit_linewalkers")
@limiter.limit("20/minute")
def edit_linewalkers(request: Request, data: List[LineWalkerItem],
                     unsealed: bool = Depends(require_unlocked),
                     auth=Depends(verify_api_key), sig=Depends(verify_signed_request)):
    save_linewalkers([item.dict() for item in data])
    return {"status": "updated", "count": len(data)}

@app.get("/view_supervisors")
def view_supervisors(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    return load_supervisors()

@app.post("/edit_supervisors")
@limiter.limit("20/minute")
def edit_supervisors(request: Request, data: List[SupervisorItem],
                     unsealed: bool = Depends(require_unlocked),
                     auth=Depends(verify_api_key), sig=Depends(verify_signed_request)):
    save_supervisors([item.dict() for item in data])
    return {"status": "updated", "count": len(data)}

@app.get("/view_npv")
def view_npv(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    return load_npv()

@app.post("/edit_npv")
@limiter.limit("20/minute")
def edit_npv(request: Request, data: List[NPVItem],
             unsealed: bool = Depends(require_unlocked),
             auth=Depends(verify_api_key), sig=Depends(verify_signed_request)):
    save_npv([item.dict() for item in data])
    return {"status": "updated", "count": len(data)}

@app.post("/reset_all_roles")
@limiter.limit("5/minute")
def reset_all_roles(request: Request,
                    unsealed: bool = Depends(require_unlocked),
                    auth=Depends(verify_api_key), sig=Depends(verify_signed_request)):
    result = {}
    try:
        with engine.begin() as conn:
            r = conn.execute(text("UPDATE linewalkers SET line_walker = '-' WHERE line_walker != '-'"))
            result["linewalkers"] = r.rowcount if hasattr(r, "rowcount") else None
            r = conn.execute(text("UPDATE supervisors SET supervisor = '-' WHERE supervisor != '-'"))
            result["supervisors"] = r.rowcount if hasattr(r, "rowcount") else None
            r = conn.execute(text("UPDATE npv_contacts SET npv = '-' WHERE npv != '-'"))
            result["npvs"] = r.rowcount if hasattr(r, "rowcount") else None
        return {"status": "reset", "details": result}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

# ---------------- Telegram utility & alerts ----------------
def get_chat_id_by_ch_and_time(ch: float) -> Optional[str]:
    mappings = load_group_mappings()
    now = datetime.now(IST).time()
    for entry in mappings:
        if entry["start_ch"] <= ch <= entry["end_ch"]:
            window = (entry["time_window"] or "").lower().strip()
            if window == "day" and dtime(6, 0) <= now <= dtime(19, 0):
                return entry["chat_id"]
            if window == "night" and (now >= dtime(19, 45) or now <= dtime(5, 45)):
                return entry["chat_id"]
    return None

def log_sent_alert(od, ch, section, line_walker, supervisor, npv):
    now = datetime.now(IST)
    db_exec("""
        INSERT INTO sent_logs 
        (Date, Time, OD, CH, Section, Linewalker, Supervisor, NPV)
        VALUES (:d, :t, :od, :ch, :sec, :lw, :sup, :npv)
    """, {
        "d": now.strftime("%Y-%m-%d"),
        "t": now.strftime("%H:%M:%S"),
        "od": float(od),
        "ch": float(ch),
        "sec": section,
        "lw": line_walker,
        "sup": supervisor,
        "npv": npv
    })

def _download_with_cap(url: str, timeout: int = 12) -> bytes:
    with requests.Session() as s:
        r = s.get(url, stream=True, timeout=timeout)
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_content(8192):
            buf.write(chunk)
            if buf.tell() > MAX_MEDIA_BYTES:
                raise RuntimeError("Media too large")
        return buf.getvalue()

def _sanitize_image_to_jpeg(data: bytes, quality: int = 85, max_pixels: int = 10_000_000) -> bytes:
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(data))
        if img.width * img.height > max_pixels:
            raise ValueError(f"Image dimensions too large: {img.width}x{img.height}")
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=quality, optimize=True)
        return out.getvalue()
    except Exception as e:
        webhook_logger.warning(f"Image processing failed, storing raw bytes: {e}")
        return data

def _is_image_mime(mime: str) -> bool:
    return (mime or "").lower().startswith("image/")

def log_duty_status(linewalker: str, duty_on: str = "", duty_off: str = ""):
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    on_raw, off_raw = (duty_on or ""), (duty_off or "")
    if not STORE_RECEIVED_TEXT:
        on_raw  = _hashlib.sha256(on_raw.encode()).hexdigest() if on_raw else ""
        off_raw = _hashlib.sha256(off_raw.encode()).hexdigest() if off_raw else ""
    db_exec(
        "INSERT INTO duty_status (Timestamp, Linewalker, Duty_on, Duty_off) "
        "VALUES (:ts, :lw, :on, :off)",
        {"ts": now, "lw": linewalker, "on": (enc_text(on_raw) or ""), "off": (enc_text(off_raw) or "")}
    )

def log_received_media(
    linewalker: str, media_type: str, file_id: str, file_unique_id: str,
    file_path: str, caption: str, file_bytes: bytes
) -> int:
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    cap_raw = caption or ""
    if not STORE_RECEIVED_TEXT:
        cap_raw = _hashlib.sha256(cap_raw.encode()).hexdigest() if cap_raw else ""
    enc_cap = enc_text(cap_raw)
    stored_bytes = enc_bytes(file_bytes) if STORE_MEDIA_BYTES else b""
    sql = text("""
        INSERT INTO received_media
            (Timestamp, Linewalker, MediaType, FileId, FileUniqueId, FilePath, Caption, Bytes)
        VALUES (:ts, :lw, :mt, :fid, :fuid, :fpath, :cap, :bytes)
        RETURNING id
    """)
    with engine.begin() as conn:
        res = conn.execute(sql, {
            "ts": now, "lw": linewalker, "mt": media_type, "fid": file_id,
            "fuid": file_unique_id, "fpath": file_path, "cap": (enc_cap or ""),
            "bytes": stored_bytes or b"",
        })
        return int(res.scalar_one())

def log_received_message(
    linewalker: str, message: str, user: str,
    media_id: int | None = None, media_type: str | None = None
):
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    thumb = f"/media/thumb?id={media_id}" if media_id else None
    raw_text = message or ""
    if not STORE_RECEIVED_TEXT:
        raw_text = _hashlib.sha256(raw_text.encode()).hexdigest() if raw_text else ""
    enc_msg = enc_text(raw_text or "")
    db_exec(
        "INSERT INTO received_messages "
        "(Timestamp, Linewalker, Message, Sender, MediaId, MediaType, MediaThumb) "
        "VALUES (:ts, :lw, :msg, :snd, :mid, :mt, :thumb)",
        {"ts": now, "lw": linewalker, "msg": (enc_msg or ""), "snd": user,
         "mid": media_id, "mt": media_type, "thumb": thumb}
    )

# ---------------- Alerts endpoint (single definitive version) ----------------
@app.post("/send_alert")
@limiter.limit("120/minute")
def send_alert(request: Request, payload: AlertPayload,
               unsealed: bool = Depends(require_unlocked),
               auth=Depends(verify_api_key), sig=Depends(verify_signed_request)):
    now_ist = datetime.now(IST)
    current_time = now_ist.time()
    ch = float(payload.ch); od = float(payload.od); section = payload.section
    line_walker = (payload.line_walker or get_linewalker_by_ch(ch) or "").strip()
    supervisor = (payload.supervisor or get_supervisor_by_ch(ch) or "").strip()
    npv = (payload.npv or get_npv_by_ch(ch) or "").strip()
    
    if dtime(6, 0) <= current_time <= dtime(13, 45):
        sender_info = f"🚶‍➡️लाइन वॉकर: {line_walker or '-'}"
        log_lw, log_sup, log_npv = (line_walker or "-", "-", "-")
    elif dtime(13, 45) < current_time <= dtime(16, 0):
        sender_info = f"🚶‍➡️लाइन वॉकर: {line_walker or '-'} 👮🏻‍♂️Sup: {supervisor or '-'}"
        log_lw, log_sup, log_npv = (line_walker or "-", supervisor or "-", "-")
    elif dtime(16, 0) < current_time <= dtime(18, 45):
        sender_info = f"👮🏻‍♂️Sup: {supervisor or '-'}"
        log_lw, log_sup, log_npv = ("-", supervisor or "-", "-")
    else:
        sender_info = f"🚗NPV: {npv or '-'}"
        log_lw, log_sup, log_npv = ("-", "-", npv or "-")
    
    msg = (
        "🔔 अलार्म सूचना 🔔\n"
        f"⏱️समय: {now_ist.strftime('%d-%m-%Y %H:%M:%S')}\n"
        f"🔎OD: {od}\n"
        f"📍CH: {ch}\n"
        f"🛣️सेक्शन: {section}\n"
        f"{sender_info}"
    )
    
    bot_token = get_setting("BOT_TOKEN")
    raw_chat_id = get_chat_id_by_ch_and_time(ch)
    
    if not bot_token:
        return JSONResponse(status_code=500, content={"status": "error", "detail": "BOT_TOKEN missing"})
    if not raw_chat_id:
        return JSONResponse(status_code=404, content={"status": "error", "detail": "No chat_id found for CH range"})
    
    cid_str = str(raw_chat_id).strip()
    cid: Union[str, int] = cid_str
    if cid_str.lstrip("-").isdigit():
        try:
            cid = int(cid_str)
        except Exception:
            pass
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        res = requests.post(
            url,
            json={"chat_id": cid, "text": msg},
            timeout=12,
            allow_redirects=False,
        )
        if res.status_code == 200:
            log_sent_alert(od, ch, section, log_lw, log_sup, log_npv)
        else:
            logger.warning({"msg": "Telegram send failed", "code": res.status_code, "text": res.text[:200]})
    except Exception as e:
        logger.exception({"msg": "Telegram send exception", "error": str(e)})
        raise HTTPException(502, detail="Telegram send failed")

    return {
        "status": "sent",
        "resolved_chat_id": cid_str,
        "resolved_window": ("day" if dtime(6,0) <= current_time <= dtime(19,0) else "night"),
        "message_preview": msg,
    }

# ---------------- Webhook (single definitive version with secret) ----------------
@app.post("/webhook")
@limiter.limit("120/minute")
async def telegram_webhook(request: Request,
                           unsealed: bool = Depends(require_unlocked)):
    header_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(header_secret, TELEGRAM_WEBHOOK_SECRET):
        record_fail("webhook_secret")
        if too_many_recent_fails():
            _set_locked(True)
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    try:
        update = await request.json()
    except Exception:
        return {"status": "ignored", "reason": "invalid json"}

    message = update.get("message") \
           or update.get("edited_message") \
           or update.get("channel_post") \
           or {}

    if not message:
        webhook_logger.info(f"Non-message update received: keys={list(update.keys())}")
        return {"status": "ignored", "reason": "no message-like payload"}

    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    user_obj = (message.get("from") or {})
    user_id = user_obj.get("id")

    def _in_allowlist(user_id: int | None, chat_id: int | None) -> bool:
        ok_user = True if not ALLOWED_USER_IDS else (user_id in ALLOWED_USER_IDS or str(user_id) in ALLOWED_USER_IDS)
        ok_chat = True if not ALLOWED_CHAT_IDS else (chat_id in ALLOWED_CHAT_IDS or str(chat_id) in ALLOWED_CHAT_IDS)
        return ok_user and ok_chat

    if not _in_allowlist(user_id, chat_id):
        return {"status": "ignored", "reason": "not allowlisted"}

    user_first = user_obj.get("first_name") or (chat.get("title") or "Unknown")
    linewalker = user_first
    text_msg = (message.get("text") or "").strip()
    caption = (message.get("caption") or "").strip()
    token = get_setting("BOT_TOKEN")

    media_id_logged: int | None = None
    media_type_logged: str | None = None

    if "photo" in message and isinstance(message["photo"], list) and message["photo"]:
        try:
            if not token:
                log_received_message(linewalker=linewalker,
                                     message="[photo received, BOT_TOKEN missing]",
                                     user=user_first)
            else:
                largest = message["photo"][-1]
                file_id = largest.get("file_id")
                file_unique_id = largest.get("file_unique_id", "")
                if file_id:
                    gf_res = requests.get(
                        f"https://api.telegram.org/bot{token}/getFile",
                        params={"file_id": file_id}, timeout=12
                    )
                    gf_res.raise_for_status()
                    file_path = gf_res.json().get("result", {}).get("file_path", "")
                    file_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
                    raw_bytes = _download_with_cap(file_url)
                    safe_bytes = _sanitize_image_to_jpeg(raw_bytes)
                    media_id_logged = log_received_media(
                        linewalker=linewalker,
                        media_type="photo",
                        file_id=file_id,
                        file_unique_id=file_unique_id,
                        file_path=file_path,
                        caption=caption,
                        file_bytes=safe_bytes
                    )
                    media_type_logged = "photo"
        except Exception as e:
            webhook_logger.exception("Photo handling failed")
            log_received_message(linewalker=linewalker,
                                 message=f"[photo exception: {e}]",
                                 user=user_first)

    elif "document" in message and isinstance(message["document"], dict):
        try:
            doc = message["document"]
            mime = (doc.get("mime_type") or "").lower()
            if _is_image_mime(mime):
                if not token:
                    log_received_message(linewalker=linewalker,
                                         message="[image_document received, BOT_TOKEN missing]",
                                         user=user_first)
                else:
                    file_id = doc.get("file_id")
                    file_unique_id = doc.get("file_unique_id", "")
                    if file_id:
                        gf_res = requests.get(
                            f"https://api.telegram.org/bot{token}/getFile",
                            params={"file_id": file_id}, timeout=12
                        )
                        gf_res.raise_for_status()
                        file_path = gf_res.json().get("result", {}).get("file_path", "")
                        file_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
                        raw_bytes = _download_with_cap(file_url)
                        safe_bytes = _sanitize_image_to_jpeg(raw_bytes)
                        media_id_logged = log_received_media(
                            linewalker=linewalker,
                            media_type="image_document",
                            file_id=file_id,
                            file_unique_id=file_unique_id,
                            file_path=file_path,
                            caption=caption,
                            file_bytes=safe_bytes
                        )
                        media_type_logged = "image_document"
        except Exception as e:
            webhook_logger.exception("Image-document handling failed")
            log_received_message(linewalker=linewalker,
                                 message=f"[image_document exception: {e}]",
                                 user=user_first)

    if text_msg:
        lower = " ".join(text_msg.lower().split())
        on_phrases = ("on", "duty on", "on duty")
        off_phrases = ("off", "of", "duty of", "duty off")

        is_on = any(p in lower for p in on_phrases)
        is_off = any(p in lower for p in off_phrases)

        if is_off:
            log_duty_status(linewalker=linewalker, duty_off=text_msg)
        elif is_on:
            log_duty_status(linewalker=linewalker, duty_on=text_msg)
        else:
            if media_id_logged:
                log_received_message(
                    linewalker=linewalker,
                    message=f"📷 [image received]",
                    user=user_first,
                    media_id=media_id_logged,
                    media_type=media_type_logged
                )
            log_received_message(linewalker=linewalker, message=text_msg, user=user_first)

    elif caption:
        if media_id_logged:
            log_received_message(
                linewalker=linewalker,
                message=f"[caption] {caption}",
                user=user_first,
                media_id=media_id_logged,
                media_type=media_type_logged
            )
        else:
            log_received_message(linewalker=linewalker, message=f"[caption] {caption}", user=user_first)

    else:
        if media_id_logged:
            log_received_message(
                linewalker=linewalker,
                message=f"📷 [image received]",
                user=user_first,
                media_id=media_id_logged,
                media_type=media_type_logged
            )

    return {"status": "received"}

# ---------------- Logs & downloads ----------------
def _df_from_query(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    with engine.begin() as conn:
        res = conn.execute(text(sql), params or {})
        rows = res.fetchall()
        cols = res.keys()
        return pd.DataFrame(rows, columns=cols)

def _select_list(table: str, columns: Iterable[str]) -> str:
    def _table_has_column(table: str, column: str) -> bool:
        try:
            with engine.begin() as conn:
                return conn.execute(
                    text("""
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = current_schema() 
                        AND table_name = :t 
                        AND column_name = :c 
                        LIMIT 1
                    """),
                    {"t": table.lower(), "c": column.lower()}
                ).scalar() is not None
        except Exception:
            return True
    parts = []
    for c in columns:
        parts.append(c if _table_has_column(table, c) else f"NULL AS {c}")
    return ", ".join(parts)

def _dec_df_text(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in ("Message", "Caption", "Duty_on", "Duty_off"):
        if col in df.columns:
            df[col] = df[col].map(dec_text)
    return df

@app.get("/get_logs")
def get_logs(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    try:
        df = _df_from_query("SELECT * FROM sent_logs ORDER BY id DESC")
        return df.to_dict(orient="records")
    except Exception as e:
        logger.exception({"msg": "get_logs failed", "error": str(e)})
        return JSONResponse(status_code=500, content={"detail": "Internal error", "why": str(e)})

@app.get("/get_media_logs")
def get_media_logs(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    try:
        df = _df_from_query("""
            SELECT id, Timestamp, Linewalker, MediaType, FileId, FileUniqueId, FilePath, Caption
            FROM received_media ORDER BY id DESC
        """)
        if "Caption" in df.columns:
            df["Caption"] = df["Caption"].map(dec_text)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.exception({"msg": "get_media_logs failed", "error": str(e)})
        return JSONResponse(status_code=500, content={"detail": "Internal error", "why": str(e)})

@app.get("/get_received_logs")
def get_received_logs(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    try:
        cols = _select_list(
            "received_messages", 
            ["id", "Timestamp", "Linewalker", "Message", "Sender", "MediaId", "MediaType", "MediaThumb"]
        )
        logs = db_query_all(f"SELECT {cols} FROM received_messages ORDER BY id DESC")
        enhanced_logs = []
        for log in logs:
            enhanced_log = dict(log)
            if "Message" in enhanced_log:
                enhanced_log["Message"] = dec_text(enhanced_log.get("Message"))
            enhanced_log.pop("MediaId", None)
            enhanced_log.pop("MediaType", None)
            enhanced_logs.append(enhanced_log)
        return enhanced_logs
    except Exception as e:
        logger.exception({"msg": "get_received_logs failed", "error": str(e)})
        return JSONResponse(status_code=500, content={"detail": "Internal error", "why": str(e)})

@app.get("/get_duty_logs")
def get_duty_logs(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    try:
        cols = _select_list("duty_status", 
            ["id", "Timestamp", "Linewalker", "Duty_on", "Duty_off"])
        df = _df_from_query(f"SELECT {cols} FROM duty_status ORDER BY id DESC")
        for col in ("Duty_on", "Duty_off"):
            if col in df.columns:
                df[col] = df[col].map(dec_text)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.exception({"msg": "get_duty_logs failed", "error": str(e)})
        return JSONResponse(status_code=500, content={"detail": "Internal error", "why": str(e)})

def _df_to_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df = _dec_df_text(df)
            df = df.copy()
            if "MediaId" in df.columns or "id" in df.columns:
                if "MediaLink" not in df.columns:
                    df["MediaLink"] = ""
                if "DownloadLink" not in df.columns:
                    df["DownloadLink"] = ""
            df.to_excel(writer, sheet_name=name[:31], index=False)
    output.seek(0)
    return output.read()

@app.get("/download_logs")
def download_logs(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    try:
        df_sent = _df_from_query("""
            SELECT id, Date, Time, OD, CH, Section, Linewalker, Supervisor, NPV 
            FROM sent_logs ORDER BY id DESC
        """)
        recv_cols = _select_list("received_messages", 
            ["id", "Timestamp", "Linewalker", "Message", "Sender", "MediaId", "MediaType", "MediaThumb"])
        df_received = _df_from_query(f"""
            SELECT {recv_cols} FROM received_messages ORDER BY id DESC
        """)
        duty_cols = _select_list("duty_status", 
            ["id", "Timestamp", "Linewalker", "Duty_on", "Duty_off"])
        df_duty = _df_from_query(f"SELECT {duty_cols} FROM duty_status ORDER BY id DESC")
        df_media = _df_from_query("""
            SELECT id, Timestamp, Linewalker, MediaType, FileId, FileUniqueId, FilePath, Caption
            FROM received_media ORDER BY id DESC
        """)

        if df_sent.empty and df_received.empty and df_duty.empty and df_media.empty:
            raise HTTPException(status_code=404, detail="No logs available to export.")
        data = _df_to_excel_bytes({
            "Sent Logs": df_sent,
            "Received Logs": df_received,
            "Duty Status": df_duty,
            "Received Media": df_media
        })
        headers = {"Content-Disposition": f'attachment; filename="{EXCEL_EXPORT_FILE}"'}
        return StreamingResponse(
            io.BytesIO(data), 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
            headers=headers
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception({"msg": "download_logs failed", "error": str(e)})
        return JSONResponse(status_code=500, content={"detail": "Internal error", "why": str(e)})

@app.get("/download_all_logs")
def download_all_logs(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    try:
        df_sent = _df_from_query("""
            SELECT id, Date, Time, OD, CH, Section, Linewalker, Supervisor, NPV 
            FROM sent_logs ORDER BY id DESC
        """)
        recv_cols = _select_list(
            "received_messages", 
            ["id", "Timestamp", "Linewalker", "Message", "Sender", "MediaId", "MediaType", "MediaThumb"]
        )
        df_received = _df_from_query(f"SELECT {recv_cols} FROM received_messages ORDER BY id DESC")
        duty_cols = _select_list("duty_status", 
            ["id", "Timestamp", "Linewalker", "Duty_on", "Duty_off"])
        df_duty = _df_from_query(f"SELECT {duty_cols} FROM duty_status ORDER BY id DESC")
        df_media = _df_from_query("""
            SELECT id, Timestamp, Linewalker, MediaType, FileId, FileUniqueId, FilePath, Caption
            FROM received_media ORDER BY id DESC
        """)
        if df_sent.empty and df_received.empty and df_duty.empty and df_media.empty:
            raise HTTPException(status_code=404, detail="No logs available to export.")
        data = _df_to_excel_bytes({
            "Sent Logs": df_sent,
            "Received Logs": df_received,
            "Duty Status": df_duty,
            "Received Media": df_media,
        })
        headers = {"Content-Disposition": 'attachment; filename="PIDS_All_Logs.xlsx"'}
        return StreamingResponse(
            io.BytesIO(data), 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
            headers=headers
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception({"msg": "download_all_logs failed", "error": str(e)})
        return JSONResponse(status_code=500, content={"detail": "Internal error", "why": str(e)})

@app.get("/download_media")
def download_media(id: int = Query(...),
                   unsealed: bool = Depends(require_unlocked),
                   auth=Depends(verify_api_key)):
    try:
        row = db_query_one("""
            SELECT id, MediaType, FilePath, Caption, Bytes 
            FROM received_media WHERE id = :id
        """, {"id": id})
        if not row:
            raise HTTPException(status_code=404, detail="Media not found")
        data: bytes = dec_bytes(row.get("Bytes") or b"") or b""
        if not data:
            token = get_setting("BOT_TOKEN")
            file_path = row.get("FilePath") or ""
            if token and file_path:
                try:
                    url = f"https://api.telegram.org/file/bot{token}/{file_path}"
                    data = _download_with_cap(url)
                except Exception:
                    pass
        if not data:
            raise HTTPException(status_code=404, detail="No media available")

        media_type = (row.get("MediaType") or "photo").lower()
        is_image = media_type in ("photo", "image_document")
        mime = "image/jpeg" if is_image else "application/octet-stream"
        filename = (row.get("FilePath") or f"media_{id}.bin").split("/")[-1]
        disp = "inline" if is_image else "attachment"
        headers = {"Content-Disposition": f'{disp}; filename="{filename}"'}
        return StreamingResponse(io.BytesIO(data), media_type=mime, headers=headers)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception({"msg": "download_media failed", "error": str(e)})
        return JSONResponse(status_code=500, content={"detail": "Internal error", "why": str(e)})

@app.get("/media/thumb")
def media_thumbnail(
    id: int = Query(..., description="received_media.id"),
    size: int = Query(256, ge=16, le=1024, description="max width/height in pixels"),
    quality: int = Query(80, ge=40, le=95, description="JPEG quality"),
    unsealed: bool = Depends(require_unlocked),
    auth=Depends(verify_api_key),
):
    try:
        row = db_query_one("""
            SELECT id, MediaType, Bytes, FilePath FROM received_media WHERE id = :id
        """, {"id": id})
        if not row:
            raise HTTPException(status_code=404, detail="Media not found")
        data: bytes = dec_bytes(row.get("Bytes") or b"") or b""
        if not data:
            token = get_setting("BOT_TOKEN")
            file_path = row.get("FilePath") or ""
            if token and file_path:
                try:
                    url = f"https://api.telegram.org/file/bot{token}/{file_path}"
                    data = _download_with_cap(url)
                except Exception:
                    pass
        if not data:
            raise HTTPException(status_code=404, detail="No media bytes stored")

        try:
            from PIL import Image, ImageDraw, ImageFont
            img = Image.open(io.BytesIO(data))
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            img.thumbnail((size, size))
            try:
                draw = ImageDraw.Draw(img)
                font = ImageFont.load_default()
                draw.text(
                    (10, 10), 
                    f"PIDSight {datetime.now(IST).strftime('%Y-%m-%d')}",
                    fill=(255, 255, 255, 128),
                    font=font
                )
            except Exception:
                pass
            out = io.BytesIO()
            img.save(out, format="JPEG", quality=quality, optimize=True)
            out.seek(0)
            return StreamingResponse(
                out, 
                media_type="image/jpeg",
                headers={
                    "Content-Disposition": f'inline; filename="thumb_{id}.jpg"',
                    "Cache-Control": "public, max-age=86400"
                }
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Thumbnailing failed: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception({"msg": "media_thumbnail failed", "error": str(e)})
        return JSONResponse(status_code=500, content={"detail": "Internal error", "why": str(e)})

# ---------------- Analytics (auth + sealed) ----------------
# Pydantic/Query regex compatibility (v1 vs v2)
try:
    _ = Query("x", regex="^x$")
    _QUERY_REGEX_KW = dict(regex="^(linewalker|supervisor|npv|section)$")
except TypeError:
    _QUERY_REGEX_KW = dict(pattern="^(linewalker|supervisor|npv|section)$")

def _normalize_sent_logs(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = df.columns.str.lower()
    if "date" in df.columns and "time" in df.columns:
        df["dt"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str), errors="coerce")
    else:
        df["dt"] = pd.NaT
    return df

@app.get("/analytics/scatter_chart")
def get_scatter_chart(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text("SELECT * FROM sent_logs"), conn)
            if df.empty:
                raise HTTPException(status_code=404, detail="No sent logs to visualize")
            df = _normalize_sent_logs(df)
            df = df.dropna(subset=["dt"])
            cutoff_naive = (datetime.now(IST) - timedelta(hours=24)).replace(tzinfo=None)
            df = df[df["dt"] >= cutoff_naive]
            if df.empty:
                raise HTTPException(status_code=404, detail="No data in the last 24 hours")
            df["ch"] = pd.to_numeric(df.get("ch"), errors="coerce")
            df = df.dropna(subset=["ch"])
            fig, ax = plt.subplots(figsize=(12, 6))
            for section, group in df.groupby(df.get("section").fillna("-")):
                ax.scatter(group["dt"], group["ch"].astype(float), label=str(section), s=40, alpha=0.7)
            ax.set_title("Chainage vs Time by Section (Last 24h)", fontsize=14)
            ax.set_xlabel("Time"); ax.set_ylabel("CH")
            if not df.get("section").isna().all():
                ax.legend()
            ax.grid(True)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            fig.autofmt_xdate()
            buf = io.BytesIO()
            fig.savefig(buf, format='png', bbox_inches="tight")
            plt.close(fig)
            buf.seek(0)
            return StreamingResponse(buf, media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception({"msg": "scatter_chart failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/analytics/group_bar_chart")
def get_group_bar_chart(group_by: str = Query("section", **_QUERY_REGEX_KW),
                        unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text("SELECT * FROM sent_logs"), conn)
            if df.empty:
                raise HTTPException(status_code=404, detail="No data available")
            df = _normalize_sent_logs(df)
            df = df.dropna(subset=["dt"])
            cutoff_naive = (datetime.now(IST) - timedelta(hours=24)).replace(tzinfo=None)
            df = df[df["dt"] >= cutoff_naive]
            if df.empty:
                raise HTTPException(status_code=404, detail="No data in the last 24 hours")
            def roles_for_time(t: dtime):
                if dtime(6, 0) <= t <= dtime(13, 45):
                    return {"linewalker"}
                elif dtime(13, 45) < t <= dtime(16, 0):
                    return {"linewalker", "supervisor"}
                elif dtime(16, 0) < t <= dtime(18, 45):
                    return {"supervisor"}
                else:
                    return {"npv"}
            df["roles"] = df["dt"].dt.time.apply(roles_for_time)
            if group_by in {"linewalker", "supervisor", "npv"}:
                col = {"linewalker": "linewalker", "supervisor": "supervisor", "npv": "npv"}[group_by]
                mask = df["roles"].apply(lambda s: group_by in s)
                df = df[mask]
                if df.empty:
                    raise HTTPException(status_code=404, detail=f"No {group_by} data in the last 24 hours")
                counts = df[col].fillna("-").astype(str).value_counts()
                title_role = group_by.capitalize()
            else:
                counts = df["section"].fillna("-").astype(str).value_counts()
                title_role = "Section"
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(counts.index.astype(str), counts.values)
            ax.set_title(f"Alert Count by {title_role} (Last 24h)")
            ax.set_ylabel("Count"); ax.set_xlabel(title_role)
            plt.xticks(rotation=45, ha="right"); plt.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format="png", bbox_inches="tight")
            plt.close(fig)
            buf.seek(0)
            return StreamingResponse(buf, media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception({"msg": "group_bar_chart failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

# ---------------- Lookups (public) ----------------
@app.get("/lookup/linewalker")
def lookup_linewalker(ch: float = Query(...)):
    name = get_linewalker_by_ch(float(ch))
    return {"linewalker": name or "-", "found": bool(name)}

@app.get("/lookup/supervisor")
def lookup_supervisor(ch: float = Query(...)):
    name = get_supervisor_by_ch(float(ch))
    return {"supervisor": name or "-", "found": bool(name)}

@app.get("/lookup/npv")
def lookup_npv(ch: float = Query(...)):
    name = get_npv_by_ch(float(ch))
    return {"npv": name or "-", "found": bool(name)}

@app.get("/lookup/all")
def lookup_all(ch: float = Query(...)):
    ch = float(ch)
    lw = get_linewalker_by_ch(ch)
    sup = get_supervisor_by_ch(ch)
    npv = get_npv_by_ch(ch)
    return {
        "linewalker": lw or "-",
        "supervisor": sup or "-",
        "npv": npv or "-",
        "found": {"linewalker": bool(lw), "supervisor": bool(sup), "npv": bool(npv)},
    }

# ---------------- Admin: webhook mgmt & refresh/migrate (auth + sealed) ----------------
@app.get("/admin/set_webhook")
@limiter.limit("5/minute")
def admin_set_webhook(request: Request, unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    token = get_setting("BOT_TOKEN")
    if not token:
        raise HTTPException(status_code=400, detail="BOT_TOKEN is not set")
    target_url = (PUBLIC_WEBHOOK_URL or "").strip()
    if not target_url:
        raise HTTPException(status_code=400, detail="PUBLIC_WEBHOOK_URL is empty")
    params = {"url": target_url}
    if TELEGRAM_WEBHOOK_SECRET:
        params["secret_token"] = TELEGRAM_WEBHOOK_SECRET
    try:
        res = requests.get(
            f"https://api.telegram.org/bot{token}/setWebhook",
            params=params,
            timeout=8
        )
        return res.json()
    except requests.RequestException as e:
        logger.exception({"msg": "set_webhook failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/admin/get_webhook_info")
@limiter.limit("10/minute")
def admin_get_webhook_info(request: Request, unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    token = get_setting("BOT_TOKEN")
    if not token:
        raise HTTPException(status_code=400, detail="BOT_TOKEN is not set")
    try:
        res = requests.get(
            f"https://api.telegram.org/bot{token}/getWebhookInfo",
            timeout=8
        )
        return res.json()
    except requests.RequestException as e:
        logger.exception({"msg": "get_webhook_info failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

def _do_refresh(req: RefreshRequest) -> Dict[str, Any]:
    result = {"status": "ok"}
    if req.reload_master:
        result["master"] = force_reload_master()
    if req.migrate_from_json:
        result["migration"] = migrate_json_to_db()
    if req.reset_webhook:
        try:
            result["webhook"] = _set_webhook_now()
        except HTTPException as e:
            result["webhook"] = {"error": e.detail}
    with engine.begin() as conn:
        def count(table: str) -> int:
            try:
                return int(conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())
            except Exception:
                return 0
        result["counts"] = {
            "group_mappings": count("group_mappings"),
            "linewalkers": count("linewalkers"),
            "supervisors": count("supervisors"),
            "npv_contacts": count("npv_contacts"),
            "sent_logs": count("sent_logs"),
        }
    return result

@app.post("/refresh")
def refresh_backend(req: RefreshRequest,
                    unsealed: bool = Depends(require_unlocked),
                    auth=Depends(verify_api_key), sig=Depends(verify_signed_request)):
    return _do_refresh(req)

@app.get("/refresh")
def refresh_backend_get(
    reload_master: bool = True,
    migrate_from_json: bool = False,
    reset_webhook: bool = False,
    unsealed: bool = Depends(require_unlocked),
    auth=Depends(verify_api_key)
):
    return _do_refresh(RefreshRequest(
        reload_master=reload_master,
        migrate_from_json=migrate_from_json,
        reset_webhook=reset_webhook
    ))

def _set_webhook_now() -> Dict[str, Any]:
    token = get_setting("BOT_TOKEN")
    if not token:
        raise HTTPException(status_code=400, detail="BOT_TOKEN is not set")
    target_url = (PUBLIC_WEBHOOK_URL or "").strip()
    if not target_url:
        raise HTTPException(status_code=400, detail="PUBLIC_WEBHOOK_URL is empty")
    params = {"url": target_url}
    if TELEGRAM_WEBHOOK_SECRET:
        params["secret_token"] = TELEGRAM_WEBHOOK_SECRET
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/setWebhook", params=params, timeout=8)
        return r.json()
    except requests.RequestException as e:
        logger.exception({"msg": "set_webhook failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/migrate_json_to_db")
def migrate_json_to_db(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    result = {}
    try:
        if os.path.exists("settings.json"):
            with open("settings.json") as f:
                for k, v in json.load(f).items():
                    set_setting(k, str(v))
            result["settings"] = "ok"
        if os.path.exists("group_mapping.json"):
            with open("group_mapping.json") as f:
                update_group_mappings(json.load(f))
            result["group_mappings"] = "ok"
        if os.path.exists("linewalkers.json"):
            with open("linewalkers.json") as f:
                save_linewalkers(json.load(f))
            result["linewalkers"] = "ok"
        if os.path.exists("supervisors.json"):
            with open("supervisors.json") as f:
                save_supervisors(json.load(f))
            result["supervisors"] = "ok"
        if os.path.exists("npv.json"):
            with open("npv.json") as f:
                save_npv(json.load(f))
            result["npv"] = "ok"
    except Exception as e:
        result["error"] = str(e)
    return result

# ---------------- Nonce GC (admin) ----------------
def gc_nonces(older_than_seconds: int = 3600):
    cutoff = datetime.utcnow() - timedelta(seconds=older_than_seconds)
    db_exec("DELETE FROM used_nonces WHERE ts < :cutoff",
            {"cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S")})

@app.post("/admin/gc_nonces")
def trigger_nonce_gc(older_than_seconds: int = Query(3600, ge=300, le=86400),
                     auth=Depends(verify_api_key)):
    gc_nonces(older_than_seconds)
    return {"status": "ok", "older_than_seconds": older_than_seconds}

# ---------------- Maintenance (auth + sealed) ----------------
@app.delete("/clear_old_logs")
@limiter.limit("5/minute")
def clear_old_logs(request: Request, days: int = Query(7, ge=1, le=365),
                   unsealed: bool = Depends(require_unlocked),
                   auth=Depends(verify_api_key),
                   sig=Depends(verify_signed_request)):
    cutoff = datetime.now(IST) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM sent_logs WHERE (Date || ' ' || Time) < :cutoff"), {"cutoff": cutoff_str})
            conn.execute(text("DELETE FROM received_messages WHERE Timestamp < :cutoff"), {"cutoff": cutoff_str})
            conn.execute(text("DELETE FROM duty_status WHERE Timestamp < :cutoff"), {"cutoff": cutoff_str})
            conn.execute(text("DELETE FROM received_media WHERE Timestamp < :cutoff"), {"cutoff": cutoff_str})
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.exec_driver_sql("VACUUM")
        return {"status": "success", "cutoff": cutoff_str}
    except Exception as e:
        logger.exception({"msg": "clear_old_logs failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.delete("/backup_and_clear")
@limiter.limit("5/minute")
def backup_and_clear(request: Request, days: int = Query(7, ge=1, le=365),
                     unsealed: bool = Depends(require_unlocked),
                     auth=Depends(verify_api_key),
                     sig=Depends(verify_signed_request)):
    try:
        with engine.begin() as conn:
            df_sent = pd.read_sql(text("SELECT * FROM sent_logs ORDER BY id DESC"), conn)
            df_received = pd.read_sql(text("SELECT * FROM received_messages ORDER BY id DESC"), conn)
            df_duty = pd.read_sql(text("SELECT * FROM duty_status ORDER BY id DESC"), conn)
            df_media = pd.read_sql(text("SELECT * FROM received_media ORDER BY id DESC"), conn)
            backup_bytes = _df_to_excel_bytes({
                "Sent Logs": df_sent,
                "Received Logs": df_received,
                "Duty Status": df_duty,
                "Received Media": df_media,
            })
    except Exception as e:
        logger.exception({"msg": "backup phase failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Backup failed")
    
    cutoff = datetime.now(IST) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM sent_logs WHERE (Date || ' ' || Time) < :cutoff"), {"cutoff": cutoff_str})
            conn.execute(text("DELETE FROM received_messages WHERE Timestamp < :cutoff"), {"cutoff": cutoff_str})
            conn.execute(text("DELETE FROM duty_status WHERE Timestamp < :cutoff"), {"cutoff": cutoff_str})
            conn.execute(text("DELETE FROM received_media WHERE Timestamp < :cutoff"), {"cutoff": cutoff_str})
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.exec_driver_sql("VACUUM")
    except Exception as e:
        headers = {
            "Content-Disposition": f'attachment; filename="PIDS_Backup_{datetime.now(IST).strftime("%Y%m%d_%H%M%S")}.xlsx"',
            "X-Cleanup-Status": f"failed: {str(e)}"
        }
        return StreamingResponse(
            io.BytesIO(backup_bytes), 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
            headers=headers
        )
    
    headers = {
        "Content-Disposition": f'attachment; filename="PIDS_Backup_{datetime.now(IST).strftime("%Y%m%d_%H%M%S")}.xlsx"',
        "X-Cleanup-Status": "ok",
        "X-Cleanup-Cutoff": cutoff_str
    }
    return StreamingResponse(
        io.BytesIO(backup_bytes), 
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
        headers=headers
    )

# ---------------- Health (public) ----------------
@app.get("/health")
def health_check():
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
        if get_setting("BOT_TOKEN"):
            try:
                requests.get(
                    f"https://api.telegram.org/bot{get_setting('BOT_TOKEN')}/getMe",
                    timeout=5
                )
            except Exception:
                pass
        return {"status": "ok", "services": ["database", "telegram"]}
    except Exception as e:
        raise HTTPException(503, detail=str(e))

# ---------------- Error shape ----------------
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    code: int = 400

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            code=exc.status_code
        ).dict()
    )

# ---------------- Admin: seal/lock control ----------------
@app.post("/admin/lock")
def admin_lock(auth=Depends(verify_api_key)):
    _set_locked(True)
    return {"status": "locked"}

@app.post("/admin/unlock")
def admin_unlock(
    secret: str = Query(..., min_length=6, description="APP_UNLOCK_SECRET"),
    auth=Depends(verify_api_key),
):
    unlock_env = os.getenv("APP_UNLOCK_SECRET", "")
    if unlock_env and not hmac.compare_digest(secret, unlock_env):
        raise HTTPException(401, "Invalid unlock secret")
    _set_locked(False)
    return {"status": "unlocked"}

# ---------------- Aliases (protected) ----------------
@app.get("/set_webhook")
def set_webhook_alias(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    return _set_webhook_now()

@app.get("/test_webhook")
def test_webhook_alias(unsealed: bool = Depends(require_unlocked), auth=Depends(verify_api_key)):
    return admin_get_webhook_info.__wrapped__(  # type: ignore
        request=None,  # not used when calling __wrapped__ directly without limiter
        unsealed=unsealed, auth=auth
    )

# ---------------- Debug helpers (protected) ----------------
@app.get("/debug/resolve_chat")
def debug_resolve_chat(
    ch: float = Query(...),
    unsealed: bool = Depends(require_unlocked),
    auth=Depends(verify_api_key),
):
    now = datetime.now(IST).time()
    window = "day" if dtime(6, 0) <= now <= dtime(19, 0) else "night"
    mappings = load_group_mappings()
    hits = [m for m in mappings if m["start_ch"] <= ch <= m["end_ch"] and (m["time_window"] or "") == window]
    return {"now": str(now), "window": window, "candidates": hits, "selected_chat_id": (hits[0]["chat_id"] if hits else None)}
# ====================== END (Consolidated) ======================
