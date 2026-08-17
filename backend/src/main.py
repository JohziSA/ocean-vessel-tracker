"""
Ocean Vessel Tracker - Backend
Polls / generates vessel data every 60s and pushes live ships via WebSocket.
Uses rich demo data (oil tankers, cargo, distressed vessels) + structure ready for AISStream.io.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vessel-tracker")

app = FastAPI(title="Ocean Vessel Tracker API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

class Vessel(BaseModel):
    id: str
    mmsi: str
    name: str
    vessel_type: str          # Tanker, Cargo, Container, Passenger, Fishing, Military, Tug, Other
    type_tag: str             # e.g. "Oil", "Bulk", "Container", "LNG", etc.
    flag: str
    latitude: float
    longitude: float
    speed_knots: float
    heading: float            # 0-359
    course: float
    navigational_status: str
    is_distress: bool = False
    destination: Optional[str] = None
    draught: Optional[float] = None
    length: Optional[float] = None
    callsign: Optional[str] = None
    imo: Optional[str] = None
    last_updated: datetime
    first_seen: datetime
    source: str = "Demo / Simulated AIS"
    confidence: float = 0.85

class SystemState(BaseModel):
    vessels: Dict[str, Vessel] = Field(default_factory=dict)
    last_poll: Optional[datetime] = None
    poll_count: int = 0
    connected_clients: int = 0

state = SystemState()
connected_websockets: Set[WebSocket] = set()

POLL_INTERVAL_SEC = 60

# ---------------------------------------------------------------------------
# Demo vessel fleet (realistic positions + movement)
# ---------------------------------------------------------------------------

DEMO_FLEET = [
    {
        "id": "demo-oil-1",
        "mmsi": "538008123",
        "name": "FRONT ALTAIR",
        "vessel_type": "Tanker",
        "type_tag": "Oil",
        "flag": "Marshall Islands",
        "lat": 25.4, "lon": 55.2,
        "speed": 12.4, "heading": 285,
        "status": "Under way using engine",
        "distress": False,
        "destination": "FUJAIRAH",
        "draught": 16.8, "length": 330, "callsign": "V7A2118", "imo": "9786543",
    },
    {
        "id": "demo-oil-2",
        "mmsi": "636019876",
        "name": "SEAWAYS RAFFLES",
        "vessel_type": "Tanker",
        "type_tag": "Oil",
        "flag": "Liberia",
        "lat": 1.2, "lon": 103.8,
        "speed": 11.1, "heading": 45,
        "status": "Under way using engine",
        "distress": False,
        "destination": "SINGAPORE",
        "draught": 15.2, "length": 274, "callsign": "D5QK8", "imo": "9712345",
    },
    {
        "id": "demo-lng-1",
        "mmsi": "311001234",
        "name": "GASLOG WARSAW",
        "vessel_type": "Tanker",
        "type_tag": "LNG",
        "flag": "Bahamas",
        "lat": 36.1, "lon": -5.4,
        "speed": 14.8, "heading": 90,
        "status": "Under way using engine",
        "distress": False,
        "destination": "BARCELONA",
        "draught": 11.5, "length": 295, "callsign": "C6YZ2", "imo": "9865432",
    },
    {
        "id": "demo-cargo-1",
        "mmsi": "477123456",
        "name": "EVER GIVEN",
        "vessel_type": "Container",
        "type_tag": "Container",
        "flag": "Panama",
        "lat": 30.0, "lon": 32.5,
        "speed": 16.2, "heading": 180,
        "status": "Under way using engine",
        "distress": False,
        "destination": "ROTTERDAM",
        "draught": 14.5, "length": 400, "callsign": "H3RC", "imo": "9811000",
    },
    {
        "id": "demo-cargo-2",
        "mmsi": "636012345",
        "name": "MSC OSCAR",
        "vessel_type": "Container",
        "type_tag": "Container",
        "flag": "Liberia",
        "lat": 35.9, "lon": -5.6,
        "speed": 18.0, "heading": 75,
        "status": "Under way using engine",
        "distress": False,
        "destination": "VALENCIA",
        "draught": 15.0, "length": 395, "callsign": "D5KJ9", "imo": "9708318",
    },
    {
        "id": "demo-bulk-1",
        "mmsi": "538007654",
        "name": "CAPE TOWN STAR",
        "vessel_type": "Cargo",
        "type_tag": "Bulk",
        "flag": "Marshall Islands",
        "lat": -33.9, "lon": 18.4,
        "speed": 10.5, "heading": 120,
        "status": "Under way using engine",
        "distress": False,
        "destination": "RICHARDS BAY",
        "draught": 17.2, "length": 289, "callsign": "V7YZ1", "imo": "9654321",
    },
    {
        "id": "demo-distress-1",
        "mmsi": "244123456",
        "name": "PACIFIC HOPE",
        "vessel_type": "Cargo",
        "type_tag": "General Cargo",
        "flag": "Netherlands",
        "lat": 22.5, "lon": 120.3,
        "speed": 0.8, "heading": 30,
        "status": "Not under command",
        "distress": True,
        "destination": "KAOHSIUNG",
        "draught": 8.4, "length": 145, "callsign": "PCDE", "imo": "9123456",
    },
    {
        "id": "demo-distress-2",
        "mmsi": "311098765",
        "name": "OCEAN STAR",
        "vessel_type": "Fishing",
        "type_tag": "Fishing",
        "flag": "Bahamas",
        "lat": 48.2, "lon": -6.1,
        "speed": 0.0, "heading": 0,
        "status": "Aground",
        "distress": True,
        "destination": None,
        "draught": 4.2, "length": 42, "callsign": "C6AB1", "imo": None,
    },
    {
        "id": "demo-passenger-1",
        "mmsi": "247123456",
        "name": "MSC SEASIDE",
        "vessel_type": "Passenger",
        "type_tag": "Cruise",
        "flag": "Italy",
        "lat": 25.8, "lon": -80.1,
        "speed": 18.5, "heading": 90,
        "status": "Under way using engine",
        "distress": False,
        "destination": "NASSAU",
        "draught": 8.5, "length": 323, "callsign": "IBPX", "imo": "9745372",
    },
    {
        "id": "demo-tug-1",
        "mmsi": "366123456",
        "name": "THOR",
        "vessel_type": "Tug",
        "type_tag": "Tug",
        "flag": "USA",
        "lat": 29.7, "lon": -95.0,
        "speed": 6.2, "heading": 210,
        "status": "Under way using engine",
        "distress": False,
        "destination": "HOUSTON",
        "draught": 5.1, "length": 38, "callsign": "WDF1234", "imo": None,
    },
    {
        "id": "demo-military-1",
        "mmsi": "369990001",
        "name": "USS EXAMPLE",
        "vessel_type": "Military",
        "type_tag": "Naval",
        "flag": "USA",
        "lat": 36.8, "lon": -76.3,
        "speed": 14.0, "heading": 45,
        "status": "Under way using engine",
        "distress": False,
        "destination": None,
        "draught": 9.0, "length": 155, "callsign": None, "imo": None,
    },
    {
        "id": "demo-fishing-1",
        "mmsi": "251234567",
        "name": "NORDIC CATCHER",
        "vessel_type": "Fishing",
        "type_tag": "Fishing",
        "flag": "Iceland",
        "lat": 64.1, "lon": -21.9,
        "speed": 7.8, "heading": 300,
        "status": "Engaged in fishing",
        "distress": False,
        "destination": "REYKJAVIK",
        "draught": 5.5, "length": 68, "callsign": "TFNA", "imo": None,
    },
]


def _move_vessel(v: dict, now: datetime) -> Vessel:
    """Slightly move demo vessels so the map feels alive."""
    lat = v["lat"]
    lon = v["lon"]
    speed = v["speed"]
    heading = v["heading"]

    if speed > 0.5 and not v["distress"]:
        # crude movement: ~0.01 deg per knot per minute (very rough)
        dist = speed * 0.00018
        import math
        rad = math.radians(heading)
        lat += dist * math.cos(rad)
        lon += dist * math.sin(rad) / max(0.2, math.cos(math.radians(lat)))
        # small random jitter
        lat += random.uniform(-0.005, 0.005)
        lon += random.uniform(-0.005, 0.005)
        speed = max(0, speed + random.uniform(-0.4, 0.4))
        heading = (heading + random.uniform(-3, 3)) % 360

    return Vessel(
        id=v["id"],
        mmsi=v["mmsi"],
        name=v["name"],
        vessel_type=v["vessel_type"],
        type_tag=v["type_tag"],
        flag=v["flag"],
        latitude=round(lat, 5),
        longitude=round(lon, 5),
        speed_knots=round(speed, 1),
        heading=round(heading, 1),
        course=round(heading, 1),
        navigational_status=v["status"],
        is_distress=v["distress"],
        destination=v.get("destination"),
        draught=v.get("draught"),
        length=v.get("length"),
        callsign=v.get("callsign"),
        imo=v.get("imo"),
        last_updated=now,
        first_seen=now - timedelta(hours=random.randint(2, 48)),
        source="Demo / Simulated AIS",
        confidence=0.9 if not v["distress"] else 0.75,
    )


def generate_demo_vessels(now: datetime) -> List[Vessel]:
    return [_move_vessel(v, now) for v in DEMO_FLEET]


# ---------------------------------------------------------------------------
# Polling & broadcast
# ---------------------------------------------------------------------------

async def poll_and_update():
    while True:
        try:
            now = datetime.now(timezone.utc)
            logger.info("Updating vessel positions...")

            vessels = generate_demo_vessels(now)

            for v in vessels:
                state.vessels[v.id] = v

            state.last_poll = now
            state.poll_count += 1
            await broadcast_state()
            logger.info("Update complete. Vessels: %d  Clients: %d", len(state.vessels), len(connected_websockets))
        except Exception as e:
            logger.exception("Poll error: %s", e)

        await asyncio.sleep(POLL_INTERVAL_SEC)


async def broadcast_state():
    if not connected_websockets:
        return
    payload = {
        "type": "state",
        "last_poll": state.last_poll.isoformat() if state.last_poll else None,
        "poll_count": state.poll_count,
        "vessels": [v.model_dump(mode="json") for v in state.vessels.values()],
    }
    dead = set()
    for ws in connected_websockets:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.add(ws)
    for ws in dead:
        connected_websockets.discard(ws)
    state.connected_clients = len(connected_websockets)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "vessels": len(state.vessels),
        "last_poll": state.last_poll.isoformat() if state.last_poll else None,
        "clients": len(connected_websockets),
        "poll_count": state.poll_count,
    }


@app.get("/api/vessels")
async def get_vessels():
    return {
        "last_poll": state.last_poll.isoformat() if state.last_poll else None,
        "vessels": [v.model_dump(mode="json") for v in state.vessels.values()],
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_websockets.add(websocket)
    state.connected_clients = len(connected_websockets)
    logger.info("Client connected. Total: %d", state.connected_clients)

    try:
        await websocket.send_json({
            "type": "state",
            "last_poll": state.last_poll.isoformat() if state.last_poll else None,
            "poll_count": state.poll_count,
            "vessels": [v.model_dump(mode="json") for v in state.vessels.values()],
        })
    except Exception:
        pass

    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                # lock/unlock acknowledgements can be handled here later
            except Exception:
                pass
    except WebSocketDisconnect:
        pass
    finally:
        connected_websockets.discard(websocket)
        state.connected_clients = len(connected_websockets)
        logger.info("Client disconnected. Total: %d", state.connected_clients)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(poll_and_update())
    # immediate first update
    now = datetime.now(timezone.utc)
    for v in generate_demo_vessels(now):
        state.vessels[v.id] = v
    state.last_poll = now
    state.poll_count = 1
    logger.info("Initial fleet loaded – %d vessels", len(state.vessels))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
