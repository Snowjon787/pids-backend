# 🚨 PIDS Alert System

A robust and real-time **Pipeline Intrusion Detection System (PIDS)** alert and monitoring solution built using **FastAPI (backend)** and **ttkbootstrap (desktop GUI frontend)**. Designed for field teams to send and receive pipeline security alerts with Telegram integration, analytics, and CH/OD conversion.

---

## 🧩 Features

### ✅ Backend (FastAPI + SQLite)
- Real-time alert sending to Telegram group
- Webhook integration (Telegram → App)
- Duty ON/OFF tracking of linewalkers
- Logs alerts, received messages, and duty status in SQLite
- Daily reset of duty status at 06:30 AM
- OD → CH and CH → OD conversion via interpolation
- API-protected `/update_token`, `/send_alert`, `/convert`, and analytics endpoints
- Generates bar and scatter charts grouped by CH, section, linewalker, fibre line
- Exports logs to Excel on demand

### ✅ Frontend (Tkinter + ttkbootstrap GUI)
- Beautiful dark-mode UI with emojis and real-time counters
- Three main tabs:
  - **📥 Alert Sender** (OD input, CH suggestion, send to Telegram)
  - **📊 Analytics** (Charts, stats, visual insights)
  - **🧮 OD ↔ CH Converter**
- Real-time status of backend
- Displays incoming messages and duty updates
- Local logging to `log.db`

---

## 🏗 Tech Stack

| Component  | Technology      |
|------------|-----------------|
| Backend    | FastAPI         |
| Frontend   | ttkbootstrap    |
| Database   | SQLite          |
| Messaging  | Telegram Bot API |
| Charts     | Matplotlib      |
| Packaging  | PyInstaller     |

---

## 🚀 How to Run Locally

### Backend
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install fastapi uvicorn pandas matplotlib openpyxl python-multipart

# Start the backend
uvicorn pids:app --reload
