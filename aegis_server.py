"""
AEGIS backend.

- Serves aegis-weather.html at http://localhost:8000
- Caches USGS earthquake and Open-Meteo weather feeds
- Watches all 36 states and union territories and pushes alerts over WebSocket
- Proxies AI chat, so ANTHROPIC_API_KEY never reaches the browser

Setup (put this file next to aegis-weather.html):
    pip install fastapi uvicorn httpx
    export ANTHROPIC_API_KEY=sk-ant-...      # optional; chat is disabled without it
    uvicorn aegis_server:app --reload
Then open http://localhost:8000
"""
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

# IST (India) timezone — Asia/Kolkata is UTC+5:30
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
    ("Odisha", 20.3, 85.8), ("Punjab", 30.7, 76.8), ("Rajasthan", 26.9, 75.8),
    ("Sikkim", 27.3, 88.6), ("Tamil Nadu", 13.1, 80.3), ("Telangana", 17.4, 78.5),
    ("Tripura", 23.8, 91.3), ("Uttar Pradesh", 26.9, 80.9), ("Uttarakhand", 30.3, 78.0),
    ("West Bengal", 22.6, 88.4), ("Andaman and Nicobar Islands", 11.6, 92.7),
    ("Chandigarh", 30.7, 76.8), ("Dadra and Nagar Haveli and Daman and Diu", 20.3, 73.0),
    ("Delhi", 28.6, 77.2), ("Jammu and Kashmir", 34.1, 74.8), ("Ladakh", 34.2, 77.6),
    ("Lakshadweep", 10.6, 72.6), ("Puducherry", 11.9, 79.8),
]

http = httpx.AsyncClient(timeout=15)
cache: dict = {}
clients: set[WebSocket] = set()
seen: dict[str, float] = {}
chat_hits: dict[str, list[float]] = {}
quake_times: list[float] = []


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
    return int(ts) if ts else 0


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


async def get_json(url, params=None):
    r = await http.get(url, params=params)
    r.raise_for_status()
    return r.json()


async def broadcast(alert: dict):
    dead = []
    for ws in clients:
        try:
            await ws.send_json(alert)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


async def monitor():
    """Every 2 minutes: check quakes for tsunami flags and weather for storms or extreme heat."""
    while True:
        try:
            feed = await cached("quakes", 120, lambda: get_json(USGS))
            for f in feed["features"]:
                p = f["properties"]
                if p.get("tsunami") and (p.get("mag") or 0) >= 6.5 and f["id"] not in seen:
                    seen[f["id"]] = time.time()
                    await broadcast({"level": "warn", "title": "Tsunami potential flagged",
                                     "detail": f"M{p['mag']:.1f} {p['place']}"})

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


class ChatIn(BaseModel):
    question: str = Field(max_length=500)
    context: dict = {}


@app.post("/api/chat")
async def chat(body: ChatIn, request: Request):
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise HTTPException(503, "Set ANTHROPIC_API_KEY on the server to enable chat")

    ip, now = request.client.host, time.time()
    recent = [t for t in chat_hits.get(ip, []) if now - t < 60]
    if len(recent) >= 20:
        raise HTTPException(429, "Too many questions. Wait a minute and try again.")
    chat_hits[ip] = recent + [now]

    r = await http.post(ANTHROPIC, headers={
        "x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json",
    }, json={
        "model": MODEL, "max_tokens": 400,
        "system": ("You are AEGIS, a calm weather and disaster-safety assistant for India. "
                   "Answer briefly using only the conditions provided. If there is an active alert "
                   "or tsunami flag, say so first and tell the user to follow official warnings from "
                   "IMD, NDMA, or INCOIS. Never invent data."),
        "messages": [{"role": "user", "content": f"Current conditions: {json.dumps(body.context)}\n\nQuestion: {body.question}"}],
    })
    if r.status_code != 200:
        raise HTTPException(502, "AI service error")
    reply = "\n".join(b["text"] for b in r.json()["content"] if b["type"] == "text")
    return {"reply": reply}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()  # keeps the connection open; clients may send pings
    except WebSocketDisconnect:
        clients.discard(ws)