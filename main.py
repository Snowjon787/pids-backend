# ===================== PIDSight Backend — Postgres Edition (Render-friendly, hardened + JSON logging) =====================
from fastapi import FastAPI, Request, Header, Depends, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse
from pydantic import BaseModel, Field
from datetime import datetime, time as dtime, timedelta
import pandas as pd

# --- Headless matplotlib + writable cache for Render BEFORE importing pyplot ---
import os
os.environ.setdefault("MPLCONFIGDIR", "/tmp")  # Render's writable tmp for font cache
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")  # keep BLAS quiet/light
import matplotlib
matplotlib.use("Agg")  # non-GUI backend for servers
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import requests
from requests.adapters import HTTPAdapter, Retry
import json
import io
import pytz
from typing import Optional, List, Dict, Any, Tuple, Union
import re
from sqlalchemy import create_engine, text

# ---- Security middleware & rate limiting ----
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

# Make slowapi optional / disabled by default
SLOWAPI_ENABLED = os.getenv("ENABLE_RATELIMIT", "0") == "1"

try:
    if SLOWAPI_ENABLED:
        from slowapi import Limiter
        from slowapi.util import get_remote_address
        from slowapi.middleware import SlowAPIMiddleware
        from slowapi.errors import RateLimitExceeded
    else:
        raise ImportError("Rate limiting disabled by flag")
except Exception:
    # Fallbacks when slowapi is missing or disabled
    class Limiter:  # no-op limiter
        def __init__(self, *a, **k): pass
        def limit(self, *a, **k):
            def deco(f): return f
            return deco

    def get_remote_address(*a, **k):
        return "0.0.0.0"

    SlowAPIMiddleware = None

    class RateLimitExceeded(Exception):
        pass

# ---- Structured logging ----
import logging
import time
import uuid

# ===================== App & Security Config =====================
IST = pytz.timezone("Asia/Kolkata")

# Secrets & config from ENV
API_KEY = os.getenv("API_KEY_PIDS", "")
if not API_KEY:
    raise RuntimeError("API_KEY_PIDS env var required")

ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
TELEGRAM_WEBHOOK_SECRET = os.getenv("TG_WEBHOOK_SECRET", "")  # optional but recommended
PUBLIC_WEBHOOK_URL = os.getenv("PUBLIC_WEBHOOK_URL", "https://pids-backend.onrender.com/webhook")

# Allowlist: comma-separated chat IDs (e.g. -1001234567890) and user IDs
def _parse_ids(raw: str) -> set:
    vals = set()
    for x in (raw or "").split(","):
        x = x.strip()
        if not x:
            continue
        if x.lstrip("-").isdigit():
            try:
                vals.add(int(x))
            except Exception:
                vals.add(x)
        else:
            vals.add(x)
    return vals

ALLOWED_CHAT_IDS = _parse_ids(os.getenv("ALLOWED_CHAT_IDS", ""))
ALLOWED_USER_IDS = _parse_ids(os.getenv("ALLOWED_USER_IDS", ""))

MAX_MEDIA_BYTES = int(os.getenv("MAX_MEDIA_BYTES", "5242880"))  # 5 MiB default
RATE_LIMIT_STORAGE_URI = os.getenv("RATE_LIMIT_STORAGE_URI", "").strip()  # e.g. redis://default:pass@host:6379/0

# ===================== Structured JSON logging =====================
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Attach request context if present
        for k in ("request_id", "method", "path", "status", "latency_ms", "client_ip"):
            if hasattr(record, k):
                payload[k] = getattr(record, k)
        # Extra fields (if any)
        if record.args and isinstance(record.args, dict):
            payload.update(record.args)
        # Include exception info if any
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(_mask_secrets(payload), ensure_ascii=False)

# Simple secret masker
_SECRET_KEYS = {"BOT_TOKEN", "API_KEY_PIDS", "TG_WEBHOOK_SECRET", "DATABASE_URL"}
_SECRET_RE = re.compile(r"(\b\d{9,}\:[A-Za-z0-9_\-]{20,}\b)|(\b[A-Za-z0-9_\-]{32,}\b)")

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
        return _SECRET_RE.sub("***", obj)
    if isinstance(obj, list):
        return [_mask_secrets(x) for x in obj]
    return obj

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

# App
app = FastAPI()

# CORS: explicit origins only (no "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "X-API-KEY"],
)

# Security headers
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

# Request ID + access logging
class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        req_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        start = time.perf_counter()
        client_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.client.host
        status = 500  # default to avoid UnboundLocalError if handler raises
        try:
            response = await call_next(request)
            status = getattr(response, "status_code", 500)
            return response
        finally:
            elapsed = (time.perf_counter() - start) * 1000.0
            rec = logging.LogRecord(
                name="pids.access", level=logging.INFO, pathname=__file__, lineno=0,
                msg=f"{request.method} {request.url.path}", args=None, exc_info=None
            )
            rec.request_id = req_id
            rec.method = request.method
            rec.path = request.url.path
            rec.status = status
            rec.latency_ms = round(elapsed, 2)
            rec.client_ip = client_ip
            logger.handle(rec)

app.add_middleware(RequestContextMiddleware)

# Rate limiting (Redis if configured) — optional
if SLOWAPI_ENABLED:
    if RATE_LIMIT_STORAGE_URI:
        limiter = Limiter(
            key_func=get_remote_address,
            storage_uri=RATE_LIMIT_STORAGE_URI,
            default_limits=["100/minute"],
            headers_enabled=True,
        )
    else:
        limiter = Limiter(
            key_func=get_remote_address,
            default_limits=["100/minute"],
            headers_enabled=True,
        )
    app.state.limiter = limiter
    if SlowAPIMiddleware is not None:
        app.add_middleware(SlowAPIMiddleware)

    @app.exception_handler(RateLimitExceeded)
    def _rate_limit_handler(request, exc):
        return PlainTextResponse("Too Many Requests", status_code=429)
else:
    limiter = Limiter()  # no-op limiter; decorators become harmless

def verify_api_key(x_api_key: str = Header(..., alias="X-API-KEY")):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

# ===================== Constants =====================
EXCEL_EXPORT_FILE = "PIDS_Log_Export.xlsx"

# ---------- Requests sessions with retry/timeouts ----------
SESSION = requests.Session()
retries = Retry(total=3, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503, 504))
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION.mount("http://", HTTPAdapter(max_retries=retries))
REQUEST_TIMEOUT = 8  # seconds

# Telegram dedicated session/timeout
TG_SESSION = requests.Session()
TG_SESSION.mount("https://", HTTPAdapter(max_retries=retries))
TG_SESSION.mount("http://", HTTPAdapter(max_retries=retries))
TELEGRAM_TIMEOUT = 12  # seconds

# ===================== Postgres (SQLAlchemy) =====================
raw_url = os.environ["DATABASE_URL"]
if raw_url.startswith("postgres://"):
    raw_url = raw_url.replace("postgres://", "postgresql+psycopg://", 1)
elif raw_url.startswith("postgresql://") and "+psycopg" not in raw_url:
    raw_url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
DATABASE_URL = raw_url

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

def init_db_pg():
    ddl = """
    -- Settings
    CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );

    -- Group mappings
    CREATE TABLE IF NOT EXISTS group_mappings (
        id SERIAL PRIMARY KEY,
        start_ch DOUBLE PRECISION,
        end_ch DOUBLE PRECISION,
        group_name TEXT,
        chat_id TEXT,
        time_window TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_group_range ON group_mappings(start_ch, end_ch, time_window);

    -- Linewalkers / Supervisors / NPV
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

    -- Logs
    CREATE TABLE IF NOT EXISTS sent_logs (
        id SERIAL PRIMARY KEY,
        Date TEXT, Time TEXT,
        OD DOUBLE PRECISION, CH DOUBLE PRECISION,
        Section TEXT, Linewalker TEXT, Supervisor TEXT, NPV TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_sent_logs_time ON sent_logs(Date, Time);
    CREATE INDEX IF NOT EXISTS ix_sent_logs_keys ON sent_logs(Section, Linewalker, Supervisor, NPV);

    CREATE TABLE IF NOT EXISTS received_messages (
        id SERIAL PRIMARY KEY,
        Timestamp TEXT, Linewalker TEXT, Message TEXT, Sender TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_received_time ON received_messages(Timestamp);

    CREATE TABLE IF NOT EXISTS duty_status (
        id SERIAL PRIMARY KEY,
        Timestamp TEXT, Linewalker TEXT, Duty_on TEXT, Duty_off TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_duty_time ON duty_status(Timestamp);

    -- Received media (images, etc.)
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
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
init_db_pg()

def db_exec(sql: str, params: Optional[Dict[str, Any]] = None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

def db_query_all(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        res = conn.execute(text(sql), params or {})
        cols = res.keys()
        return [dict(zip(cols, row)) for row in res.fetchall()]

def db_query_one(sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    rows = db_query_all(sql, params)
    return rows[0] if rows else None

# ===================== Master CSV Cache (HOT RELOAD) =====================
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

# ===================== Interpolation =====================
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

# ===================== Settings & Masters =====================
def get_setting(key: str) -> Optional[str]:
    row = db_query_one("SELECT value FROM app_settings WHERE key = :k", {"k": key})
    return row["value"] if row else None

def set_setting(key: str, value: str) -> None:
    db_exec("""
        INSERT INTO app_settings(key, value) VALUES(:k, :v)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """, {"k": key, "v": value})

def load_settings() -> Dict[str, str]:
    keys = ["BOT_TOKEN", "ALERT_SOUND", "TIMEOUT"]
    return {k: (get_setting(k) or "") for k in keys}

# ===================== Group Mapping =====================
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
            "start_ch": start_ch, "end_ch": end_ch, "group_name": group_name,
            "chat_id": chat_id, "time_window": time_window
        })
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM group_mappings"))
        conn.execute(text("""
            INSERT INTO group_mappings (start_ch, end_ch, group_name, chat_id, time_window)
            VALUES (:start_ch, :end_ch, :group_name, :chat_id, :time_window)
        """), normalized)

# ===================== Linewalkers / Supervisors / NPV =====================
def load_linewalkers():
    return db_query_all("SELECT start_ch, end_ch, line_walker FROM linewalkers")

def save_linewalkers(data: List[Dict[str, Any]]):
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    rows = [{"start_ch": d["start_ch"], "end_ch": d["end_ch"], "line_walker": d["line_walker"], "saved_at": now} for d in data]
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM linewalkers"))
        conn.execute(text("""
            INSERT INTO linewalkers (start_ch, end_ch, line_walker, saved_at)
            VALUES (:start_ch, :end_ch, :line_walker, :saved_at)
        """), rows)

def load_supervisors():
    return db_query_all("SELECT start_ch, end_ch, supervisor FROM supervisors")

def save_supervisors(data: List[Dict[str, Any]]):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM supervisors"))
        conn.execute(text("""
            INSERT INTO supervisors (start_ch, end_ch, supervisor)
            VALUES (:start_ch, :end_ch, :supervisor)
        """), data)

def load_npv():
    return db_query_all("SELECT start_ch, end_ch, npv FROM npv_contacts")

def save_npv(data: List[Dict[str, Any]]):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM npv_contacts"))
        conn.execute(text("""
            INSERT INTO npv_contacts (start_ch, end_ch, npv)
            VALUES (:start_ch, :end_ch, :npv)
        """), data)

# ---- CH-based lookup helpers ----
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

# ---- Seed from env on first boot (idempotent) ----
def _maybe_seed_from_env():
    if os.getenv("SEED_ON_START", "0") != "1":
        return
    try:
        tok = os.getenv("SEED_BOT_TOKEN", "").strip()
        if tok and not get_setting("BOT_TOKEN"):
            set_setting("BOT_TOKEN", tok)

        gm_raw = os.getenv("SEED_GROUP_MAPPINGS_JSON", "").strip()
        if gm_raw:
            try:
                gm = json.loads(gm_raw)
                existed = db_query_one("SELECT 1 FROM group_mappings LIMIT 1")
                if not existed and isinstance(gm, list):
                    update_group_mappings(gm)
            except Exception:
                pass

        def _seed_list(env_key: str, table: str, saver):
            raw = os.getenv(env_key, "").strip()
            if not raw:
                return
            existed = db_query_one(f"SELECT 1 FROM {table} LIMIT 1")
            if existed:
                return
            try:
                data = json.loads(raw)
                if isinstance(data, list) and data:
                    saver(data)
            except Exception:
                pass

        _seed_list("SEED_LINEWALKERS_JSON", "linewalkers", save_linewalkers)
        _seed_list("SEED_SUPERVISORS_JSON", "supervisors", save_supervisors)
        _seed_list("SEED_NPV_JSON", "npv_contacts", save_npv)

    except Exception as e:
        logger.exception({"msg": "Seeding error", "error": str(e)})
_maybe_seed_from_env()

# ===================== Migration (from JSON files on disk) =====================
@app.get("/migrate_json_to_db")
def migrate_json_to_db():
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

# ===================== Models =====================
class TokenUpdateRequest(BaseModel):
    token: str = Field(..., min_length=1)

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

# ===================== Section Endpoints =====================
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

@app.get("/get_sections")
def get_sections():
    try:
        section_data = get_section_data()
        return list(section_data.keys())
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ===================== API Endpoints (Settings & Masters) =====================
# 🔐 KEEP AUTH
@app.post("/update_token")
@limiter.limit("20/minute")
def update_token(request: Request, data: TokenUpdateRequest, auth=Depends(verify_api_key)):
    set_setting("BOT_TOKEN", data.token.strip())
    return {"status": "success", "message": "Bot token updated"}

# 🔐 KEEP AUTH
@app.post("/update_group_mappings")
@limiter.limit("20/minute")
def update_group_mappings_endpoint(request: Request, payload: List[GroupMappingItem], auth=Depends(verify_api_key)):
    update_group_mappings([item.dict() for item in payload])
    return {"status": "success", "updated_count": len(payload)}

@app.get("/view_linewalkers")
def view_linewalkers():
    return load_linewalkers()

@app.post("/edit_linewalkers")
@limiter.limit("20/minute")
def edit_linewalkers(request: Request, data: List[LineWalkerItem]):
    save_linewalkers([item.dict() for item in data])
    return {"status": "updated", "count": len(data)}

@app.get("/view_supervisors")
def view_supervisors():
    return load_supervisors()

@app.post("/edit_supervisors")
@limiter.limit("20/minute")
def edit_supervisors(request: Request, data: List[SupervisorItem]):
    save_supervisors([item.dict() for item in data])
    return {"status": "updated", "count": len(data)}

@app.get("/view_npv")
def view_npv():
    return load_npv()

@app.post("/edit_npv")
@limiter.limit("20/minute")
def edit_npv(request: Request, data: List[NPVItem]):
    save_npv([item.dict() for item in data])
    return {"status": "updated", "count": len(data)}

# ===================== Reset Roles =====================
@app.post("/reset_all_roles")
@limiter.limit("5/minute")
def reset_all_roles(request: Request):
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

# ===================== Telegram Utility =====================
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

# ===================== Alerts =====================
class AlertPayload(BaseModel):
    od: float = Field(..., ge=0, le=10_000_000)
    ch: float = Field(..., ge=0, le=10_000_000)
    section: str = Field(..., min_length=1, max_length=100)
    line_walker: str = Field("", max_length=100)
    supervisor: Optional[str] = Field("", max_length=100)
    npv: Optional[str] = Field("", max_length=100)

def log_sent_alert(od, ch, section, line_walker, supervisor, npv):
    now = datetime.now(IST)
    db_exec("""
        INSERT INTO sent_logs (Date, Time, OD, CH, Section, Linewalker, Supervisor, NPV)
        VALUES (:d, :t, :od, :ch, :sec, :lw, :sup, :npv)
    """, {
        "d": now.strftime("%Y-%m-%d"),
        "t": now.strftime("%H:%M:%S"),
        "od": float(od), "ch": float(ch),
        "sec": section, "lw": line_walker, "sup": supervisor, "npv": npv
    })

def _send_and_log_alert_async(
    bot_token: str,
    chat_id: Union[str, int],
    msg: str,
    od: float,
    ch: float,
    section: str,
    log_lw: str,
    log_sup: str,
    log_npv: str,
):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        res = TG_SESSION.post(
            url,
            json={"chat_id": chat_id, "text": msg},
            timeout=TELEGRAM_TIMEOUT,
            allow_redirects=False,
        )
        if res.status_code == 200:
            log_sent_alert(od, ch, section, log_lw or "-", log_sup or "-", log_npv or "-")
        else:
            logger.warning({"msg": "Telegram send failed", "code": res.status_code, "text": res.text[:200]})
    except Exception as e:
        logger.exception({"msg": "Telegram send exception", "error": str(e)})

@app.post("/send_alert")
@limiter.limit("120/minute")
def send_alert(request: Request, payload: AlertPayload, background_tasks: BackgroundTasks):
    now_ist = datetime.now(IST)
    current_time = now_ist.time()
    ch = float(payload.ch); od = float(payload.od); section = payload.section

    line_walker = (payload.line_walker or get_linewalker_by_ch(ch) or "").strip()
    supervisor  = (payload.supervisor  or get_supervisor_by_ch(ch)  or "").strip()
    npv         = (payload.npv         or get_npv_by_ch(ch)         or "").strip()

    if dtime(6, 0) <= current_time <= dtime(13, 45):
        sender_info = f"🚶‍➡️लाइन वॉकर: {line_walker or '-'}"
        log_lw, log_sup, log_npv = (line_walker or "-", "-", "-")
    elif dtime(13, 45) < current_time <= dtime(16, 0):
        sender_info = f"🚶‍➡️लाइन वॉकर: {line_walker or '-'}  👮🏼Sup: {supervisor or '-'}"
        log_lw, log_sup, log_npv = (line_walker or "-", supervisor or "-", "-")
    elif dtime(16, 0) < current_time <= dtime(18, 45):
        sender_info = f"👮🏼Sup: {supervisor or '-'}"
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
        try: cid = int(cid_str)
        except Exception: pass

    background_tasks.add_task(
        _send_and_log_alert_async, bot_token, cid, msg, od, ch, section, log_lw, log_sup, log_npv
    )
    return {
        "status": "queued",
        "resolved_chat_id": cid_str,
        "resolved_window": ("day" if dtime(6,0) <= current_time <= dtime(19,0) else "night"),
        "message_preview": msg,
    }

# ===================== Logs =====================
@app.get("/get_logs")
def get_logs():
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text("SELECT * FROM sent_logs ORDER BY id DESC"), conn)
        if df.empty: return []
        df["dt"] = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce")
        df = df.dropna(subset=["dt"])
        cutoff_naive = (datetime.now(IST) - timedelta(hours=48)).replace(tzinfo=None)
        df = df[df["dt"] >= cutoff_naive].drop(columns=["dt"])
        return df.to_dict(orient="records")
    except Exception as e:
        logger.exception({"msg": "get_logs failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

def _df_to_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)
    output.seek(0)
    return output.read()

@app.get("/download_logs")
def download_logs():
    try:
        with engine.begin() as conn:
            df_sent = pd.read_sql(text("SELECT * FROM sent_logs ORDER BY id DESC"), conn)
            df_received = pd.read_sql(text("SELECT * FROM received_messages ORDER BY id DESC"), conn)
            df_duty = pd.read_sql(text("SELECT * FROM duty_status ORDER BY id DESC"), conn)
        if df_sent.empty and df_received.empty and df_duty.empty:
            raise HTTPException(status_code=404, detail="No logs available to export.")
        data = _df_to_excel_bytes({
            "Sent Logs": df_sent, "Received Logs": df_received, "Duty Status": df_duty
        })
        headers = {"Content-Disposition": f'attachment; filename="{EXCEL_EXPORT_FILE}"'}
        return StreamingResponse(io.BytesIO(data),
                                 media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                 headers=headers)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception({"msg": "download_logs failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

def log_received_message(linewalker: str, message: str, user: str):
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    db_exec(
        "INSERT INTO received_messages (Timestamp, Linewalker, Message, Sender) VALUES (:ts, :lw, :msg, :snd)",
        {"ts": now, "lw": linewalker, "msg": message, "snd": user}
    )

def log_duty_status(linewalker: str, duty_on: str = "", duty_off: str = ""):
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    db_exec(
        "INSERT INTO duty_status (Timestamp, Linewalker, Duty_on, Duty_off) VALUES (:ts, :lw, :on, :off)",
        {"ts": now, "lw": linewalker, "on": duty_on, "off": duty_off}
    )

# ---------- Received media (images) ----------
def log_received_media(
    linewalker: str, media_type: str, file_id: str, file_unique_id: str,
    file_path: str, caption: str, file_bytes: bytes
):
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    sql = text("""
        INSERT INTO received_media
            (Timestamp, Linewalker, MediaType, FileId, FileUniqueId, FilePath, Caption, Bytes)
        VALUES (:ts, :lw, :mt, :fid, :fuid, :fpath, :cap, :bytes)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "ts": now, "lw": linewalker, "mt": media_type, "fid": file_id,
            "fuid": file_unique_id, "fpath": file_path, "cap": caption or "", "bytes": file_bytes,
        })

@app.get("/get_media_logs")
def get_media_logs():
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text("""
                SELECT id, Timestamp, Linewalker, MediaType, FileId, FileUniqueId, FilePath, Caption
                FROM received_media ORDER BY id DESC
            """), conn)
        if df.empty: return []
        df["dt"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df = df.dropna(subset=["dt"])
        cutoff_naive = (datetime.now(IST) - timedelta(hours=48)).replace(tzinfo=None)
        df = df[df["dt"] >= cutoff_naive].drop(columns=["dt"])
        return df.to_dict(orient="records")
    except Exception as e:
        logger.exception({"msg": "get_media_logs failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/download_media")
def download_media(id: int = Query(...)):
    try:
        row = db_query_one("""
            SELECT id, MediaType, FilePath, Caption, Bytes
            FROM received_media WHERE id = :id
        """, {"id": id})
        if not row:
            raise HTTPException(status_code=404, detail="Media not found")
        data: bytes = row.get("Bytes") or b""
        if not data:
            raise HTTPException(status_code=404, detail="No media bytes stored")
        media_type = row.get("MediaType") or "photo"
        mime = "image/jpeg" if media_type in ("photo", "image_document") else "application/octet-stream"
        filename = (row.get("FilePath") or f"media_{id}.bin").split("/")[-1]
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(io.BytesIO(data), media_type=mime, headers=headers)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception({"msg": "download_media failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/media/thumb")
def media_thumbnail(
    id: int = Query(..., description="received_media.id"),
    size: int = Query(256, ge=16, le=1024, description="max width/height in pixels"),
    quality: int = Query(80, ge=40, le=95, description="JPEG quality"),
):
    try:
        row = db_query_one("""
            SELECT id, MediaType, Bytes
            FROM received_media
            WHERE id = :id
        """, {"id": id})
        if not row:
            raise HTTPException(status_code=404, detail="Media not found")
        data: bytes = row.get("Bytes") or b""
        if not data:
            raise HTTPException(status_code=404, detail="No media bytes stored")

        try:
            from PIL import Image
        except Exception:
            raise HTTPException(status_code=500, detail="Pillow (PIL) not installed on server")

        try:
            img = Image.open(io.BytesIO(data))
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            img.thumbnail((size, size))
            out = io.BytesIO()
            img.save(out, format="JPEG", quality=quality, optimize=True)
            out.seek(0)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Thumbnailing failed: {e}")

        return StreamingResponse(out, media_type="image/jpeg")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception({"msg": "media_thumbnail failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/get_received_logs")
def get_received_logs():
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text("SELECT * FROM received_messages ORDER BY id DESC"), conn)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.exception({"msg": "get_received_logs failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/get_duty_logs")
def get_duty_logs():
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text("SELECT * FROM duty_status ORDER BY id DESC"), conn)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.exception({"msg": "get_duty_logs failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/download_all_logs")
def download_all_logs():
    try:
        with engine.begin() as conn:
            df_sent = pd.read_sql(text("SELECT * FROM sent_logs ORDER BY id DESC"), conn)
            df_received = pd.read_sql(text("SELECT * FROM received_messages ORDER BY id DESC"), conn)
            df_duty = pd.read_sql(text("SELECT * FROM duty_status ORDER BY id DESC"), conn)
            df_media = pd.read_sql(text("""
                SELECT id, Timestamp, Linewalker, MediaType, FileId, FileUniqueId, FilePath, Caption
                FROM received_media ORDER BY id DESC
            """), conn)
        if df_sent.empty and df_received.empty and df_duty.empty and df_media.empty:
            raise HTTPException(status_code=404, detail="No logs available to export.")
        data = _df_to_excel_bytes({
            "Sent Logs": df_sent, "Received Logs": df_received,
            "Duty Status": df_duty, "Received Media": df_media,
        })
        headers = {"Content-Disposition": 'attachment; filename="PIDS_All_Logs.xlsx"'}
        return StreamingResponse(io.BytesIO(data),
                                 media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                 headers=headers)
    except Exception as e:
        logger.exception({"msg": "download_all_logs failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

# ===================== Telegram helpers (secure) =====================
def _download_with_cap(url: str, timeout: int = TELEGRAM_TIMEOUT) -> bytes:
    with TG_SESSION.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_content(8192):
            buf.write(chunk)
            if buf.tell() > MAX_MEDIA_BYTES:
                raise RuntimeError("Media too large")
        return buf.getvalue()

def _sanitize_image_to_jpeg(data: bytes, quality: int = 85) -> bytes:
    try:
        from PIL import Image
    except Exception:
        return data
    try:
        img = Image.open(io.BytesIO(data))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=quality, optimize=True)
        out.seek(0)
        return out.read()
    except Exception:
        return data

def _is_image_mime(mime: str) -> bool:
    return (mime or "").lower().startswith("image/")

def _in_allowlist(user_id: Optional[int], chat_id: Optional[int]) -> bool:
    ok_user = True if not ALLOWED_USER_IDS else (user_id in ALLOWED_USER_IDS)
    ok_chat = True if not ALLOWED_CHAT_IDS else (chat_id in ALLOWED_CHAT_IDS)
    return ok_user and ok_chat

# ===================== Telegram Webhook (IMAGE-AWARE & SECURE) =====================
@app.post("/webhook")
@limiter.limit("120/minute")
async def telegram_webhook(request: Request):
    # Verify Telegram secret header if configured
    if TELEGRAM_WEBHOOK_SECRET:
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != TELEGRAM_WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Invalid webhook secret")

    try:
        data = await request.json()
    except Exception:
        return {"status": "ignored"}

    message = data.get("message", {}) or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    user_obj = (message.get("from") or {})
    user_id = user_obj.get("id")

    # Enforce allowlists if provided
    if not _in_allowlist(user_id, chat_id):
        return {"status": "ignored", "reason": "not allowlisted"}

    user_first = user_obj.get("first_name", "Unknown")
    linewalker = user_first

    text_msg = (message.get("text") or "").strip()
    caption = (message.get("caption") or "").strip()

    token = get_setting("BOT_TOKEN")

    # ====== 1) Media handling (photos and image documents) ======
    if "photo" in message and isinstance(message["photo"], list) and message["photo"]:
        try:
            photo_sizes = message["photo"]
            largest = photo_sizes[-1]
            file_id = largest.get("file_id")
            file_unique_id = largest.get("file_unique_id", "")
            if file_id and token:
                gf_url = f"https://api.telegram.org/bot{token}/getFile"
                gf_res = TG_SESSION.get(gf_url, params={"file_id": file_id}, timeout=TELEGRAM_TIMEOUT)
                if gf_res.status_code == 200:
                    file_path = gf_res.json().get("result", {}).get("file_path", "")
                    file_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
                    raw_bytes = _download_with_cap(file_url)
                    safe_bytes = _sanitize_image_to_jpeg(raw_bytes)  # strip EXIF + compress
                    log_received_media(
                        linewalker=linewalker,
                        media_type="photo",
                        file_id=file_id,
                        file_unique_id=file_unique_id,
                        file_path=file_path,
                        caption=caption,
                        file_bytes=safe_bytes
                    )
            elif not token:
                log_received_message(linewalker=linewalker, message="[photo received, BOT_TOKEN missing]", user=user_first)
        except Exception as e:
            log_received_message(linewalker=linewalker, message=f"[photo exception: {e}]", user=user_first)

    elif "document" in message and isinstance(message["document"], dict):
        try:
            doc = message["document"]
            mime = (doc.get("mime_type") or "").lower()
            if _is_image_mime(mime):
                file_id = doc.get("file_id")
                file_unique_id = doc.get("file_unique_id", "")
                if file_id and token:
                    gf_url = f"https://api.telegram.org/bot{token}/getFile"
                    gf_res = TG_SESSION.get(gf_url, params={"file_id": file_id}, timeout=TELEGRAM_TIMEOUT)
                    if gf_res.status_code == 200:
                        file_path = gf_res.json().get("result", {}).get("file_path", "")
                        file_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
                        raw_bytes = _download_with_cap(file_url)
                        safe_bytes = _sanitize_image_to_jpeg(raw_bytes)
                        log_received_media(
                            linewalker=linewalker,
                            media_type="image_document",
                            file_id=file_id,
                            file_unique_id=file_unique_id,
                            file_path=file_path,
                            caption=caption,
                            file_bytes=safe_bytes
                        )
        except Exception as e:
            log_received_message(linewalker=linewalker, message=f"[image_document exception: {e}]", user=user_first)

    # ====== 2) Text + duty tracking ======
    if text_msg:
        lower = " ".join(text_msg.lower().split())
        on_phrases  = ("duty on", "on duty")
        off_phrases = ("duty off", "off duty")
        tokens = set(re.findall(r"\b\w+\b", lower))
        is_on  = any(p in lower for p in on_phrases) or ("on"  in tokens)
        is_off = any(p in lower for p in off_phrases) or ("off" in tokens) or ("of" in tokens)

        if is_off:
            log_duty_status(linewalker=linewalker, duty_off=text_msg)
        elif is_on:
            log_duty_status(linewalker=linewalker, duty_on=text_msg)
        else:
            log_received_message(linewalker=linewalker, message=text_msg, user=user_first)
    elif caption:
        log_received_message(linewalker=linewalker, message=f"[caption] {caption}", user=user_first)

    return {"status": "received"}

# ===================== MAINTENANCE: Cleanup & Backup =====================
@app.delete("/clear_old_logs")
@limiter.limit("5/minute")
def clear_old_logs(request: Request, days: int = Query(7, ge=1, le=365)):
    cutoff = datetime.now(IST) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM sent_logs WHERE (Date || ' ' || Time) < :cutoff"), {"cutoff": cutoff_str})
            conn.execute(text("DELETE FROM received_messages WHERE Timestamp < :cutoff"), {"cutoff": cutoff_str})
            conn.execute(text("DELETE FROM duty_status WHERE Timestamp < :cutoff"), {"cutoff": cutoff_str})
            conn.execute(text("DELETE FROM received_media WHERE Timestamp < :cutoff"), {"cutoff": cutoff_str})
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.exec_driver_sql("VACUUM FULL")
        return {"status": "success", "cutoff": cutoff_str}
    except Exception as e:
        logger.exception({"msg": "clear_old_logs failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.delete("/backup_and_clear")
@limiter.limit("5/minute")
def backup_and_clear(request: Request, days: int = Query(7, ge=1, le=365)):
    try:
        with engine.begin() as conn:
            df_sent = pd.read_sql(text("SELECT * FROM sent_logs ORDER BY id DESC"), conn)
            df_received = pd.read_sql(text("SELECT * FROM received_messages ORDER BY id DESC"), conn)
            df_duty = pd.read_sql(text("SELECT * FROM duty_status ORDER BY id DESC"), conn)
            df_media = pd.read_sql(text("""
                SELECT id, Timestamp, Linewalker, MediaType, FileId, FileUniqueId, FilePath, Caption
                FROM received_media ORDER BY id DESC
            """), conn)
        backup_bytes = _df_to_excel_bytes({
            "Sent Logs": df_sent, "Received Logs": df_received,
            "Duty Status": df_duty, "Received Media": df_media,
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
            conn.exec_driver_sql("VACUUM FULL")
    except Exception as e:
        headers = {
            "Content-Disposition": f'attachment; filename="PIDS_Backup_{datetime.now(IST).strftime("%Y%m%d_%H%M%S")}.xlsx"',
            "X-Cleanup-Status": f"failed: {str(e)}"
        }
        return StreamingResponse(io.BytesIO(backup_bytes),
                                 media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                 headers=headers)

    headers = {
        "Content-Disposition": f'attachment; filename="PIDS_Backup_{datetime.now(IST).strftime("%Y%m%d_%H%M%S")}.xlsx"',
        "X-Cleanup-Status": "ok",
        "X-Cleanup-Cutoff": cutoff_str
    }
    return StreamingResponse(io.BytesIO(backup_bytes),
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers=headers)

# ===================== Webhook Management =====================
@app.get("/set_webhook")
@limiter.limit("5/minute")
def set_telegram_webhook(request: Request):
    token = get_setting("BOT_TOKEN")
    if not token:
        raise HTTPException(status_code=400, detail="BOT_TOKEN is not set")
    params = {"url": PUBLIC_WEBHOOK_URL}
    if TELEGRAM_WEBHOOK_SECRET:
        params["secret_token"] = TELEGRAM_WEBHOOK_SECRET
    try:
        res = SESSION.get(f"https://api.telegram.org/bot{token}/setWebhook", params=params, timeout=REQUEST_TIMEOUT)
        return res.json()
    except requests.RequestException as e:
        logger.exception({"msg": "set_webhook failed", "error": str(e)})
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/ping")
def ping():
    return {"status": "ok", "message": "PIDS backend is alive."}

@app.get("/test_webhook")
@limiter.limit("10/minute")
def test_webhook(request: Request):
    token = get_setting("BOT_TOKEN")
    if not token:
        return {"status": "error", "detail": "BOT_TOKEN not set"}
    url = f"https://api.telegram.org/bot{token}/getWebhookInfo"
    try:
        response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        return response.json()
    except Exception as e:
        logger.exception({"msg": "test_webhook failed", "error": str(e)})
        return {"status": "error", "detail": "Internal error"}

# ===================== Analytics =====================
@app.get("/analytics/scatter_chart")
def get_scatter_chart():
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text("SELECT * FROM sent_logs"), conn)
        if df.empty:
            raise HTTPException(status_code=404, detail="No sent logs to visualize")
        df["dt"] = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce")
        df = df.dropna(subset=["dt"])
        cutoff_naive = (datetime.now(IST) - timedelta(hours=24)).replace(tzinfo=None)
        df = df[df["dt"] >= cutoff_naive]
        if df.empty:
            raise HTTPException(status_code=404, detail="No data in the last 24 hours")
        df["CH"] = pd.to_numeric(df["CH"], errors="coerce")
        df = df.dropna(subset=["CH"])
        fig, ax = plt.subplots(figsize=(12, 6))
        for section, group in df.groupby("Section"):
            ax.scatter(group["dt"], group["CH"].astype(float), label=section, s=40, alpha=0.7)
        ax.set_title("Chainage vs Time by Section (Last 24h)", fontsize=14)
        ax.set_xlabel("Time"); ax.set_ylabel("CH")
        ax.legend(); ax.grid(True)
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
def get_group_bar_chart(group_by: str = Query("linewalker", pattern="^(linewalker|supervisor|npv|section)$")):
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text("SELECT * FROM sent_logs"), conn)
        if df.empty:
            raise HTTPException(status_code=404, detail="No data available")
        df["dt"] = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce")
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
            col = {"linewalker": "Linewalker", "supervisor": "Supervisor", "npv": "NPV"}[group_by]
            mask = df["roles"].apply(lambda s: group_by in s)
            df = df[mask]
            if df.empty:
                raise HTTPException(status_code=404, detail=f"No {group_by} data in the last 24 hours")
            counts = df[col].fillna("-").astype(str).value_counts()
            title_role = group_by.capitalize()
        else:
            counts = df["Section"].fillna("-").astype(str).value_counts()
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

# ===================== Lookup Endpoints =====================
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

# ===================== Refresh Endpoints =====================
@app.post("/refresh")
def refresh_backend(req: RefreshRequest):
    result = {"status": "ok"}
    if req.reload_master:
        result["master"] = force_reload_master()
    if req.migrate_from_json:
        result["migration"] = migrate_json_to_db()
    if req.reset_webhook:
        try:
            result["webhook"] = set_telegram_webhook(Request)
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

@app.get("/refresh")
def refresh_backend_get(
    reload_master: bool = True, migrate_from_json: bool = False, reset_webhook: bool = False,
):
    return refresh_backend(RefreshRequest(
        reload_master=reload_master, migrate_from_json=migrate_from_json, reset_webhook=reset_webhook
    ))

# ===================== Debug Helpers =====================
@app.get("/debug/resolve_chat")
def debug_resolve_chat(ch: float = Query(...)):
    now = datetime.now(IST).time()
    window = "day" if dtime(6, 0) <= now <= dtime(19, 0) else "night"
    mappings = load_group_mappings()
    hits = [m for m in mappings if m["start_ch"] <= ch <= m["end_ch"] and (m["time_window"] or "") == window]
    return {"now": str(now), "window": window, "candidates": hits, "selected_chat_id": (hits[0]["chat_id"] if hits else None)}

@app.get("/debug/telegram_chat")
def debug_telegram_chat(chat_id: str = Query(...)):
    bot_token = get_setting("BOT_TOKEN")
    if not bot_token:
        raise HTTPException(400, "BOT_TOKEN not set")
    cid = str(chat_id).strip()
    if cid.lstrip("-").isdigit():
        try: cid = int(cid)
        except Exception: pass
    r = requests.get(f"https://api.telegram.org/bot{bot_token}/getChat", params={"chat_id": cid})
    try:
        return r.json()
    except Exception:
        return {"code": r.status_code, "raw": r.text}
