import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

HERE = Path(__file__).parent
USGS = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
ANTHROPIC = "https://api.anthropic.com/v1/messages"
MODEL = "claude-haiku-5-5"

# AI Provider config
AI_PROVIDER = os.environ.get("AI_PROVIDER", "ollama").lower()
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2:3b")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-latest")
HAS_ANTHROPIC_KEY = bool(os.environ.get("ANTHROPIC_API_KEY"))

IST = ZoneInfo("Asia/Kolkata")
IST_OFFSET = timedelta(hours=5, minutes=30)

# Same order as the STATES list in aegis-weather.html
PLACES = [
    ("Andhra Pradesh", 16.5, 80.5), ("Arunachal Pradesh", 27.1, 93.6), ("Assam", 26.1, 91.8),
    ("Bihar", 25.6, 85.1), ("Chhattisgarh", 21.25, 81.6), ("Goa", 15.5, 73.8),
    ("Gujarat", 23.2, 72.6), ("Haryana", 30.7, 76.8), ("Himachal Pradesh", 31.1, 77.2),
    ("Jharkhand", 23.3, 85.3), ("Karnataka", 13.0, 77.6), ("Kerala", 8.5, 76.9),
    ("Madhya Pradesh", 23.3, 77.4), ("Maharashtra", 19.1, 72.9), ("Manipur", 24.8, 93.9),
    ("Meghalaya", 25.6, 91.9), ("Mizoram", 23.7, 92.7), ("Nagaland", 25.7, 94.1),
    ("Angul", 20.84, 85.10), ("Balangir", 20.71, 83.49), ("Balasore", 21.49, 86.93), ("Bargarh", 21.33, 83.62), ("Bhadrak", 21.06, 86.50), ("Boudh", 20.84, 84.32), ("Cuttack", 20.46, 85.88), ("Deogarh", 21.54, 84.73), ("Dhenkanal", 20.65, 85.60), ("Gajapati", 19.16, 84.18), ("Ganjam", 19.38, 84.97), ("Jagatsinghpur", 20.25, 86.17), ("Jajpur", 20.85, 86.34), ("Jharsuguda", 21.85, 84.01), ("Kalahandi", 19.91, 83.17), ("Kandhamal", 20.14, 84.05), ("Kendrapara", 20.50, 86.42), ("Keonjhar", 21.63, 85.58), ("Khordha", 20.18, 85.62), ("Koraput", 18.81, 82.71), ("Malkangiri", 18.35, 81.89), ("Mayurbhanj", 21.93, 86.73), ("Nabarangpur", 19.24, 82.55), ("Nayagarh", 20.13, 85.10), ("Nuapada", 20.82, 82.55), ("Puri", 19.81, 85.83), ("Rayagada", 19.17, 83.42), ("Sambalpur", 21.47, 83.97), ("Subarnapur", 20.84, 83.92), ("Sundargarh", 22.12, 84.02), ("Punjab", 30.7, 76.8), ("Rajasthan", 26.9, 75.8),
    ("Sikkim", 27.3, 88.6), ("Tamil Nadu", 13.1, 80.3), ("Telangana", 17.4, 78.5),
    ("Tripura", 23.8, 91.3), ("Uttar Pradesh", 26.9, 80.9), ("Uttarakhand", 30.3, 78.0),
    ("West Bengal", 22.6, 88.4), ("Andaman and Nicobar Islands", 11.6, 92.7),
    ("Chandigarh", 30.7, 76.8), ("Dadra and Nagar Haveli and Daman and Diu", 20.3, 73.0),
    ("Delhi", 28.6, 77.2), ("Jammu and Kashmir", 34.1, 74.8), ("Ladakh", 34.2, 77.6),
    ("Lakshadweep", 10.6, 72.6), ("Puducherry", 11.9, 79.8),
]


cache = {}

async def get_json(url, params=None):
    """Fetch JSON from URL."""
    r = await http.get(url, params=params)
    r.raise_for_status()
    return r.json()
clients = set()
seen = {}
chat_hits = {}
http = httpx.AsyncClient(timeout=15)

async def cached(key, ttl, fetch):
    """Return a cached value if it is younger than ttl seconds, otherwise refetch."""
    if key in cache and time.time() - cache[key][0] < ttl:
        return cache[key][1]
    data = await fetch()
    cache[key] = (time.time(), data)
    return data


def to_ist(utc_ts):
    """Convert a UTC timestamp (string ISO or numeric ms epoch) to IST."""
    if isinstance(utc_ts, (int, float)):
        dt = datetime.fromtimestamp(utc_ts / 1000, tz=timezone.utc)
    else:
        dt = datetime.fromisoformat(str(utc_ts).replace("Z", "+00:00"))
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")


def get_quake_epoch_ms(prop):
    """Extract earthquake timestamp in milliseconds from USGS properties."""
    ts = prop.get("time")
    if isinstance(ts, (int, float)):
        return ts
    return int(datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp() * 1000)


def compute_timing_analytics(feed):
    """Build real-time analytics for quakes using Indian (IST) timings."""
    now = time.time()
    quakes = feed.get("features", [])
    recent = []
    for f in quakes:
        ts_ms = get_quake_epoch_ms(f["properties"])
        ts_sec = ts_ms / 1000.0
        if now - ts_sec < 3600:
            recent.append(f)
    last = quakes[-1] if quakes else None
    rate = round((len(recent) / 3600) * 60, 1) if recent else 0.0
    return {
        "total_quakes": len(quakes),
        "quakes_last_60min": len(recent),
        "rate_per_hour": rate,
        "last_quake_ist": to_ist(last["properties"]["time"]) if last else None,
        "last_quake_mag": last["properties"]["mag"] if last else None,
        "last_quake_place": last["properties"]["place"] if last else None,
        "last_updated_ist": datetime.now(IST).strftime("%H:%M:%S IST"),
    }


async def broadcast(alert: dict):
    """Send alert to all connected WebSocket clients."""
    dead = []
    for ws in clients:
        try:
            await ws.send_json(alert)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)
# ============================================================
# AI Provider — tries Ollama (free local) → Gemini (free) → Anthropic → rule-based
# ============================================================
async def call_ai(question: str, context: dict) -> str:
    """Try AI providers in order; return best available response."""
    provider = AI_PROVIDER

    # 1. Ollama (free local AI)
    if provider == "ollama":
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(
                    f"{OLLAMA_URL}/api/chat",
                    json={
                        "model": OLLAMA_MODEL,
                        "messages": [
                            {"role": "system", "content": (
                                "You are AEGIS, a calm Indian weather and disaster-safety assistant. "
                                "Answer in simple Hindi and English together. "
                                "Give a 2-3 sentence weather summary plus 1 actionable tip. "
                                "Be short and clear."
                            )},
                            {"role": "user", "content": f"Current conditions: {json.dumps(context)}\n\nQuestion: {question}"},
                        ],
                        "stream": False,
                    },
                )
                if r.status_code == 200:
                    reply = r.json().get("message", {}).get("content", "")
                    if reply:
                        return reply
        except Exception:
            pass

    # 2. Google Gemini (free tier)
    if provider == "gemini" and GEMINI_API_KEY:
        try:
            r = await http.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
                json={
                    "contents": [{
                        "role": "user",
                        "parts": [{"text": f"Current conditions: {json.dumps(context)}\n\nQuestion: {question}"}],
                    }],
                    "systemInstruction": {
                        "role": "system",
                        "parts": [{
                            "text": (
                                "You are AEGIS, a calm Indian weather and disaster-safety assistant. "
                                "Answer in simple Hindi and English together. "
                                "Give a 2-3 sentence weather summary plus 1 actionable tip. "
                                "If there is an active alert or tsunami flag, say so first and tell the user to follow "
                                "official warnings from IMD, NDMA, or INCOIS. Never invent data; use only the provided conditions."
                            ),
                        }],
                    },
                    "generationConfig": {"maxOutputTokens": 600},
                },
                headers={"Content-Type": "application/json"},
            )
            if r.status_code == 200:
                reply = r.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                if reply:
                    return reply
        except Exception as e:
            import traceback
            traceback.print_exc()
            pass

    # 3. Anthropic (API key)
    if provider == "anthropic" and HAS_ANTHROPIC_KEY:
        try:
            r = await http.post(ANTHROPIC, headers={
                "x-api-key": os.environ["ANTHROPIC_API_KEY"],
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }, json={
                "model": MODEL, "max_tokens": 600,
                "system": (
                    "You are AEGIS, a calm Indian weather and disaster-safety assistant. "
                    "Answer in simple Hindi and English together. "
                    "Give a 2-3 sentence weather summary plus 1 actionable tip. "
                    "If there is an active alert or tsunami flag, say so first and tell the user to follow "
                    "official warnings from IMD, NDMA, or INCOIS. Never invent data; use only the provided conditions."
                ),
                "messages": [{"role": "user", "content": f"Current conditions: {json.dumps(context)}\n\nQuestion: {question}"}],
            })
            if r.status_code == 200:
                return "\n".join(b["text"] for b in r.json()["content"] if b["type"] == "text")
        except Exception:
            pass

    # 4. Rule-based fallback (always works, no API needed)
    return rule_based_response(context, question)


def rule_based_response(context: dict, question: str) -> str:
    """No-API fallback: generates helpful responses from weather data."""
    loc = context.get("location", "yha")
    temp = context.get("temperature_c", 25)
    cond = context.get("condition", "").lower()
    risk = context.get("risk_index", 0)
    alert = context.get("active_alert")

    lines = []

    # Temperature based advice
    if temp >= 40:
        lines.append(f"🌡️ {loc} mein aaj {temp}°C se zyada, bojh lene wali garmi hai. Zyaada paani piyein, noon tak bahar mat jaiye.")
    elif temp >= 35:
        lines.append(f"☀️ {loc} mein {temp}°C hai — thodi garam, paani aur thanda khaana lo.")
    elif temp >= 25:
        lines.append(f"🌤️ {loc} mein {temp}°C, aaram bhari sukoon wali mahol hai.")
    else:
        lines.append(f"❄️ {loc} mein {temp}°C hai — thandi lagi hai, lambi kapde peheniye.")

    # Condition based advice
    if "rain" in cond or "storm" in cond:
        lines.append("🌧️ Barf/Toofan ke waqt zaroor zaroorit raahon par jaayein, underpass aur road ki taraf se door rahein.")
    elif "cloudy" in cond or "overcast" in cond:
        lines.append("☁️ Baarish shayad aayegi, umbrella lein.")
    elif "clear" in cond or "sunny" in cond:
        lines.append("☀️ Clear sky — roz subah 30-minute walk karne ka sahi samay hai.")

    # Risk based advice
    if risk >= 70:
        lines.append("⚠️ Calamity risk high hai — safed zone mein rahein, family ko contact rakhein.")
    elif risk >= 40:
        lines.append("📋 Calamity risk moderate hai — kuch precautions lena na bhoolen.")

    # Alert (real-time warning)
    if alert:
        lvl = alert.get("level", "alert")
        title = alert.get("title", "")
        detail = alert.get("detail", "")
        if lvl in ("emerg", "emergency"):
            lines.insert(0, f"🚨 EMERGENCY: {title} — {detail}. ABHI baat karne se kam, bachne ke liye karein.")
        elif lvl == "warn":
            lines.insert(0, f"⚠️ Warning: {title} — {detail}")
        elif lvl == "adv":
            lines.insert(0, f"📢 Advisory: {title} — {detail}")

    # Question routing
    q = question.lower()
    if "safe" in q and "travel" in q:
        lines.append("✈️ Travel: Agli din weather normal hai, safe rahega. Lekin last-minute official updates check karein.")
    elif "heat" in q or "cool" in q:
        lines.append("🥤 Cool karne ke liye paani, juice, aur AC wali cheezon ka use karein. Thandi siitron lein.")
    elif "rain" in q:
        lines.append("🌂 Barf wali cheezein: umbrella, raincoat, shoes. Road wet ho sakta hai.")
    elif "pack" in q or "what to bring" in q:
        lines.append("🎒 Packing list: light cotton clothes, water bottle, sunscreen, cap, and a light jacket.")
    elif "workout" in q or "gym" in q:
        lines.append("🏃 Workout: " + ("Subah 5-7 baje jaldi karein, afternoon mein avoid karein." if temp > 35 else "Thodi late tareekh mein tackle karein."))
    elif "air" in q or "pollution" in q or "mask" in q:
        lines.append("😷 Air quality bad hai — khaas kar subah-jagaan, N95 mask use karein.")

    if not lines:
        lines.append(f"✅ {loc} mein aaj {temp}°C, {cond}. Safer rahein, paani piyein.")

    return " ".join(lines)

async def wxassist(body: dict, request: Request):
    """AI weather assistance — free with no API key."""
    try:
        # Handle both dict and Pydantic model
        if hasattr(body, "question"):
            question = body.question.strip()
            context = body.context
        else:
            question = body.get("question", "").strip()
            context = body.get("context", {})
        if not question:
            raise HTTPException(400, "Empty question")
        return {"reply": await call_ai(question, context)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


class ChatIn(BaseModel):
    question: str = Field(max_length=500)
    context: dict = {}


class WXAssistIn(BaseModel):
    question: str = Field(max_length=500)
    context: dict = Field(default_factory=dict)


async def monitor():
    """Every 2 minutes: check quakes for tsunami flags and weather for storms or extreme heat."""
    while True:
        try:
            feed = await cached("quakes", 120, lambda: get_json(USGS))
            for f in feed.get("features", []):
                p = f.get("properties", {})
                if p.get("tsunami") and (p.get("mag") or 0) >= 6.5:
                    ts = get_quake_epoch_ms(p)
                    if ts not in seen:
                        seen[ts] = time.time()
                        await broadcast({
                            "level": "warn",
                            "title": "Tsunami potential flagged",
                            "detail": f"M{p['mag']:.1f} {p['place']}",
                        })

            lats = ",".join(str(p[1]) for p in PLACES)
            lons = ",".join(str(p[2]) for p in PLACES)
            data = await cached("weather_all", 600, lambda: get_json(
                OPEN_METEO, {"latitude": lats, "longitude": lons, "current": "temperature_2m,weather_code"}))
            rows = data if isinstance(data, list) else [data]
            for (name, _, _), row in zip(PLACES, rows):
                c = row["current"]
                if c["weather_code"] >= 95 or c["temperature_2m"] >= 42:
                    kind = "storm" if c["weather_code"] >= 95 else "heat"
                    key = f"{name}:{kind}"
                    if time.time() - seen.get(key, 0) > 3 * 3600:
                        seen[key] = time.time()
                        title = f"Thunderstorm warning, {name}" if kind == "storm" else f"Extreme heat, {name}"
                        await broadcast({"level": "warn" if kind == "storm" else "adv", "title": title,
                                         "detail": f"{c['temperature_2m']} C at last check"})
        except Exception as e:
            print("monitor error:", e)
        await asyncio.sleep(30)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(monitor())
    yield
    task.cancel()
    await http.aclose()


app = FastAPI(title="AEGIS backend", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/")
async def home():
    page = HERE / "aegis-weather.html"
    if not page.exists():
        raise HTTPException(404, "Put aegis-weather.html in the same folder as this file")
    return FileResponse(page)


@app.get("/api/quakes")
async def api_quakes():
    feed = await cached("quakes", 120, lambda: get_json(USGS))
    return [{
        "id": f["id"], "mag": f["properties"]["mag"], "place": f["properties"]["place"],
        "time": to_ist(f["properties"]["time"]), "tsunami": bool(f["properties"].get("tsunami")),
        "lon": f["geometry"]["coordinates"][0], "lat": f["geometry"]["coordinates"][1],
        "depth": f["geometry"]["coordinates"][2],
    } for f in feed["features"]]


@app.get("/api/timing")
async def api_timing():
    feed = await cached("quakes", 120, lambda: get_json(USGS))
    return {
        "ist_time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        "offset": "+05:30",
        "zone": "Asia/Kolkata (IST)",
        "analytics": compute_timing_analytics(feed),
    }


@app.get("/api/weather")
async def api_weather(lat: float, lon: float):
    params = {
        "latitude": lat, "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,pressure_msl,weather_code",
        "hourly": "temperature_2m",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,uv_index_max,sunrise,sunset",
        "timezone": "auto", "forecast_days": 7,
    }
    return await cached(f"wx:{lat:.2f},{lon:.2f}", 600, lambda: get_json(OPEN_METEO, params))


@app.post("/api/chat")
async def chat(body: ChatIn, request: Request):
    return await wxassist(body, request)


@app.post("/api/wxassist")
async def wxassist_route(body: WXAssistIn, request: Request):
    return await wxassist(body, request)


class ImageIn(BaseModel):
    image: str
    mime_type: str = "image/jpeg"
    question: str = "Analyze this weather/sky photo and give safety advice."


@app.post("/api/analyze-image")
async def analyze_image(body: ImageIn):
    if not GEMINI_API_KEY:
        raise HTTPException(503, "Gemini API key not configured")
    try:
        r = await http.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
            json={
                "contents": [{
                    "parts": [
                        {"inline_data": {"mime_type": body.mime_type, "data": body.image}},
                        {"text": body.question},
                    ]
                }],
                "systemInstruction": {"role": "system", "parts": [{"text": "You are AEGIS weather vision assistant. Analyze sky/weather photos: clouds, rain, storm signs, visibility, and give safety advice in Hindi+English. Be concise."}]},
                "generationConfig": {"maxOutputTokens": 600},
            },
            headers={"Content-Type": "application/json"},
        )
        if r.status_code != 200:
            raise HTTPException(502, f"Gemini error: {r.text[:200]}")
        reply = r.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        if not reply:
            raise HTTPException(502, "No reply from Gemini")
        return {"reply": reply}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
