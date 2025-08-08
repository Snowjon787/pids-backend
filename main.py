# PIDSight Backend — Postgres Edition (Render-friendly, no ephemeral data loss)
from fastapi import FastAPI, Request, Header, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from datetime import datetime, time as dtime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests
from requests.adapters import HTTPAdapter, Retry
import json
import io
import os
import pytz
from typing import Optional, List, Dict, Any, Tuple, Union
import re
from sqlalchemy import create_engine, text

# ===================== App & Config =====================
app = FastAPI()

EXCEL_EXPORT_FILE = "PIDS_Log_Export.xlsx"
API_KEY = "Yj@mb51"
IST = pytz.timezone("Asia/Kolkata")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

# ---------- Requests session with retry/timeouts ----------
SESSION = requests.Session()
retries = Retry(total=3, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503, 504))
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION.mount("http://", HTTPAdapter(max_retries=retries))
REQUEST_TIMEOUT = 8  # seconds

# ===================== Postgres (SQLAlchemy) =====================
# Normalize Render's URL to psycopg v3 (avoid psycopg2 dependency)
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
        print(f"❌ Error loading {_MASTER_FILE}: {e}")
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
        # 1) BOT token
        tok = os.getenv("SEED_BOT_TOKEN", "").strip()
        if tok and not get_setting("BOT_TOKEN"):
            set_setting("BOT_TOKEN", tok)

        # 2) Group mappings
        gm_raw = os.getenv("SEED_GROUP_MAPPINGS_JSON", "").strip()
        if gm_raw:
            try:
                gm = json.loads(gm_raw)
                # Only seed if table is empty
                existed = db_query_one("SELECT 1 FROM group_mappings LIMIT 1")
                if not existed and isinstance(gm, list):
                    update_group_mappings(gm)
            except Exception:
                pass

        # 3) Linewalkers / Supervisors / NPV
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
        print("Seeding error:", e)

# Call seeding AFTER saver functions exist
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
@app.post("/update_token")
def update_token(data: TokenUpdateRequest, auth=Depends(verify_api_key)):
    set_setting("BOT_TOKEN", data.token.strip())
    return {"status": "success", "message": "Bot token updated"}

@app.post("/update_group_mappings")
def update_group_mappings_endpoint(payload: List[GroupMappingItem], auth=Depends(verify_api_key)):
    update_group_mappings([item.dict() for item in payload])
    return {"status": "success", "updated_count": len(payload)}

@app.get("/view_linewalkers")
def view_linewalkers():
    return load_linewalkers()

@app.post("/edit_linewalkers")
def edit_linewalkers(data: List[LineWalkerItem], auth=Depends(verify_api_key)):
    save_linewalkers([item.dict() for item in data])
    return {"status": "updated", "count": len(data)}

@app.get("/view_supervisors")
def view_supervisors():
    return load_supervisors()

@app.post("/edit_supervisors")
def edit_supervisors(data: List[SupervisorItem], auth=Depends(verify_api_key)):
    save_supervisors([item.dict() for item in data])
    return {"status": "updated", "count": len(data)}

@app.get("/view_npv")
def view_npv():
    return load_npv()

@app.post("/edit_npv")
def edit_npv(data: List[NPVItem], auth=Depends(verify_api_key)):
    save_npv([item.dict() for item in data])
    return {"status": "updated", "count": len(data)}

# ===================== Reset Roles =====================
@app.post("/reset_all_roles")
def reset_all_roles():
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
    od: float
    ch: float
    section: str
    line_walker: str = ""       # allow empty; we will auto-fill
    supervisor: Optional[str] = ""
    npv: Optional[str] = ""

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

@app.post("/send_alert")
def send_alert(payload: AlertPayload):
    now_ist = datetime.now(IST)
    current_time = now_ist.time()
    ch = float(payload.ch)
    od = float(payload.od)
    section = payload.section

    # Use provided values if present, else look up by CH
    line_walker = (payload.line_walker or get_linewalker_by_ch(ch) or "").strip()
    supervisor = (payload.supervisor or get_supervisor_by_ch(ch) or "").strip()
    npv = (payload.npv or get_npv_by_ch(ch) or "").strip()

    # Sender selection by time window
    if dtime(6, 0) <= current_time <= dtime(13, 45):
        sender_info = f"🚶‍➡️लाइन वॉकर: {line_walker or '-'}"
    elif dtime(13, 45) < current_time <= dtime(16, 0):
        sender_info = f"🚶‍➡️लाइन वॉकर: {line_walker or '-'}  👨‍💼Sup: {supervisor or '-'}"
    elif dtime(16, 0) < current_time <= dtime(18, 45):
        sender_info = f"👨‍💼Sup: {supervisor or '-'}"
    elif current_time >= dtime(18, 45) or current_time <= dtime(5, 45):
        sender_info = f"🛡️NPV: {npv or '-'}"
    else:
        sender_info = "🚨Contact unavailable for this time window"

    # Multiline Telegram message
    msg = (
        "🔔 अलार्म सूचना 🔔\n"
        f"⏱️समय: {now_ist.strftime('%d-%m-%Y %H:%M:%S')}\n"
        f"🔎OD: {od}\n"
        f"📍CH: {ch}\n"
        f"📈सेक्शन: {section}\n"
        f"{sender_info}"
    )

    bot_token = get_setting("BOT_TOKEN")
    raw_chat_id = get_chat_id_by_ch_and_time(ch)

    if not bot_token:
        return JSONResponse(status_code=500, content={"status": "error", "detail": "BOT_TOKEN missing"})
    if not raw_chat_id:
        return JSONResponse(status_code=404, content={"status": "error", "detail": "No chat_id found for CH range"})

    # Normalize chat_id for Telegram API (trim, cast numeric -100... to int)
    cid: Union[str, int]
    cid_str = str(raw_chat_id).strip()
    if cid_str.lstrip("-").isdigit():
        try:
            cid = int(cid_str)
        except Exception:
            cid = cid_str
    else:
        cid = cid_str

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        res = SESSION.post(url, json={"chat_id": cid, "text": msg}, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as e:
        return JSONResponse(status_code=500, content={
            "status": "error",
            "detail": str(e),
            "resolved_chat_id": cid_str,
            "resolved_window": ("day" if dtime(6,0) <= current_time <= dtime(19,0) else "night")
        })

    if res.status_code == 200:
        log_sent_alert(od, ch, section, line_walker or "-", supervisor or "-", npv or "-")
        return {
            "status": "success",
            "message_id": res.json().get("result", {}).get("message_id"),
            "resolved_chat_id": cid_str,
            "resolved_window": ("day" if dtime(6,0) <= current_time <= dtime(19,0) else "night")
        }
    else:
        return JSONResponse(status_code=500, content={
            "status": "error",
            "detail": res.text,
            "resolved_chat_id": cid_str,
            "resolved_window": ("day" if dtime(6,0) <= current_time <= dtime(19,0) else "night")
        })

# ===================== Logs =====================
@app.get("/get_logs")
def get_logs():
    try:
        df = pd.read_sql("SELECT * FROM sent_logs ORDER BY id DESC", engine)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
        df_sent = pd.read_sql("SELECT * FROM sent_logs ORDER BY id DESC", engine)
        df_received = pd.read_sql("SELECT * FROM received_messages ORDER BY id DESC", engine)
        df_duty = pd.read_sql("SELECT * FROM duty_status ORDER BY id DESC", engine)

        if df_sent.empty and df_received.empty and df_duty.empty:
            raise HTTPException(status_code=404, detail="No logs available to export.")

        data = _df_to_excel_bytes({
            "Sent Logs": df_sent,
            "Received Logs": df_received,
            "Duty Status": df_duty
        })
        headers = {"Content-Disposition": f'attachment; filename="{EXCEL_EXPORT_FILE}"'}
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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

@app.get("/get_received_logs")
def get_received_logs():
    try:
        df = pd.read_sql("SELECT * FROM received_messages ORDER BY id DESC", engine)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_duty_logs")
def get_duty_logs():
    try:
        df = pd.read_sql("SELECT * FROM duty_status ORDER BY id DESC", engine)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download_all_logs")
def download_all_logs():
    try:
        df_sent = pd.read_sql("SELECT * FROM sent_logs ORDER BY id DESC", engine)
        df_received = pd.read_sql("SELECT * FROM received_messages ORDER BY id DESC", engine)
        df_duty = pd.read_sql("SELECT * FROM duty_status ORDER BY id DESC", engine)

        if df_sent.empty and df_received.empty and df_duty.empty:
            raise HTTPException(status_code=404, detail="No logs available to export.")

        data = _df_to_excel_bytes({
            "Sent Logs": df_sent,
            "Received Logs": df_received,
            "Duty Status": df_duty
        })
        headers = {"Content-Disposition": 'attachment; filename="PIDS_All_Logs.xlsx"'}
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ===================== Telegram Webhook =====================
@app.post("/webhook")
async def telegram_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        return {"status": "ignored"}

    message = data.get("message", {}) or {}
    text = (message.get("text") or "").strip()
    user = (message.get("from") or {}).get("first_name", "Unknown")
    if not text:
        return {"status": "ignored"}

    lower = " ".join(text.lower().split())
    linewalker = user

    on_phrases  = ("duty on", "on duty")
    off_phrases = ("duty off", "off duty")

    tokens = set(re.findall(r"\b\w+\b", lower))

    is_on  = any(p in lower for p in on_phrases) or ("on"  in tokens)
    is_off = any(p in lower for p in off_phrases) or ("off" in tokens) or ("of" in tokens)

    if is_off:
        log_duty_status(linewalker=linewalker, duty_off=text)
    elif is_on:
        log_duty_status(linewalker=linewalker, duty_on=text)
    else:
        log_received_message(linewalker=linewalker, message=text, user=user)

    return {"status": "received"}

# ===================== Webhook Management =====================
@app.get("/set_webhook")
def set_telegram_webhook():
    token = get_setting("BOT_TOKEN")
    if not token:
        raise HTTPException(status_code=400, detail="BOT_TOKEN is not set")
    url = f"https://api.telegram.org/bot{token}/setWebhook"
    webhook_url = "https://pids-backend.onrender.com/webhook"
    try:
        res = SESSION.get(url, params={"url": webhook_url}, timeout=REQUEST_TIMEOUT)
        return res.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ping")
def ping():
    return {"status": "ok", "message": "PIDS backend is alive."}

@app.get("/test_webhook")
def test_webhook():
    token = get_setting("BOT_TOKEN")
    if not token:
        return {"status": "error", "detail": "BOT_TOKEN not set"}
    url = f"https://api.telegram.org/bot{token}/getWebhookInfo"
    try:
        response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        return response.json()
    except Exception as e:
        return {"status": "error", "detail": str(e)}

# ===================== Analytics =====================
@app.get("/analytics/scatter_chart")
def get_scatter_chart():
    try:
        df = pd.read_sql("SELECT * FROM sent_logs", engine)
        if df.empty:
            raise HTTPException(status_code=404, detail="No sent logs to visualize")

        # Build local (IST) datetime and filter last 24 hours
        df["dt"] = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce")
        df = df.dropna(subset=["dt"])
        cutoff = (datetime.now(IST) - timedelta(hours=24)).replace(tzinfo=None)
        df = df[df["dt"] >= cutoff]

        if df.empty:
            raise HTTPException(status_code=404, detail="No data in the last 24 hours")

        # Ensure CH numeric
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
        plt.savefig(buf, format='png', bbox_inches="tight")
        plt.close(fig); buf.seek(0)
        return StreamingResponse(buf, media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/group_bar_chart")
def get_group_bar_chart(group_by: str = Query("linewalker", pattern="^(linewalker|supervisor|npv|section)$")):
    try:
        df = pd.read_sql("SELECT * FROM sent_logs", engine)
        if df.empty:
            raise HTTPException(status_code=404, detail="No data available")

        df["dt"] = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce")
        df = df.dropna(subset=["dt"])
        cutoff = (datetime.now(IST) - timedelta(hours=24)).replace(tzinfo=None)
        df = df[df["dt"] >= cutoff]
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
        plt.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig); buf.seek(0)
        return StreamingResponse(buf, media_type="image/png")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
        "found": {
            "linewalker": bool(lw),
            "supervisor": bool(sup),
            "npv": bool(npv),
        },
    }

# ===================== Refresh Endpoints =====================
@app.post("/refresh")
def refresh_backend(req: RefreshRequest):
    """
    Refresh in-memory state without restarting the server.
    """
    result = {"status": "ok"}

    if req.reload_master:
        result["master"] = force_reload_master()

    if req.migrate_from_json:
        result["migration"] = migrate_json_to_db()

    if req.reset_webhook:
        try:
            result["webhook"] = set_telegram_webhook()
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
    reload_master: bool = True,
    migrate_from_json: bool = False,
    reset_webhook: bool = False
):
    return refresh_backend(RefreshRequest(
        reload_master=reload_master,
        migrate_from_json=migrate_from_json,
        reset_webhook=reset_webhook
    ))

# ===================== Debug Helpers =====================
@app.get("/debug/resolve_chat")
def debug_resolve_chat(ch: float = Query(...)):
    now = datetime.now(IST).time()
    window = "day" if dtime(6, 0) <= now <= dtime(19, 0) else "night"
    mappings = load_group_mappings()
    hits = [m for m in mappings if m["start_ch"] <= ch <= m["end_ch"] and (m["time_window"] or "") == window]
    return {
        "now": str(now),
        "window": window,
        "candidates": hits,
        "selected_chat_id": (hits[0]["chat_id"] if hits else None)
    }

@app.get("/debug/telegram_chat")
def debug_telegram_chat(chat_id: str = Query(...)):
    bot_token = get_setting("BOT_TOKEN")
    if not bot_token:
        raise HTTPException(400, "BOT_TOKEN not set")
    cid = str(chat_id).strip()
    if cid.lstrip("-").isdigit():
        try:
            cid = int(cid)
        except Exception:
            pass
    r = requests.get(f"https://api.telegram.org/bot{bot_token}/getChat", params={"chat_id": cid})
    try:
        return r.json()
    except Exception:
        return {"code": r.status_code, "raw": r.text}
