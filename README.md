# Ocean Vessel Tracker

Near-real-time web application for monitoring ships worldwide. Shows vessel type tags (Oil/Tanker, Container, Cargo, etc.), speed, heading, destination, and clearly flags vessels in distress. Supports locking onto any ship so the map follows it.

## Features

- Updates every **60 seconds**
- Interactive MapLibre map
- Vessel list with filters: All / Oil·Tanker / Container / Cargo / Passenger / **Distress**
- Lock-on tracking (map follows the selected ship)
- Detailed panel: speed (knots), heading, type tag, flag, MMSI/IMO, destination, draught, distress status
- Dark maritime-themed UI

## Architecture

- **Backend** (`backend/src/main.py`): FastAPI + WebSockets  
  - Refreshes vessel state every 60 seconds  
  - Rich demo fleet (oil tankers, LNG, container ships, cargo, fishing, military, + distressed vessels) that slowly move  
  - Ready to plug in a free [aisstream.io](https://aisstream.io) API key later for real AIS data  

- **Frontend** (`frontend/index.html`): Single-page MapLibre app  
  - Live WebSocket connection  
  - Color-coded markers + pulsing effect on distress vessels  
  - Sidebar filters and lock controls  

## Running (Windows / Python 3.12 recommended)

### Backend
```bash
cd ocean-vessel-tracker/backend
py -3.12 -m pip install -r requirements.txt
cd src
py -3.12 -m uvicorn main:app --host 0.0.0.0 --port 8001
```

### Frontend (new terminal)
```bash
cd ocean-vessel-tracker/frontend
py -3.12 -m http.server 5174
```

Open: **http://localhost:5174**

> Note: Backend uses port **8001** (so it can run side-by-side with the missile tracker on 8000).

## Important Limitations (Truthful)

- Full global real-time AIS is commercial. This version ships with high-quality **demo vessels** so every feature (lock, oil tags, distress) works immediately.
- Real AIS can be added later via the free aisstream.io WebSocket (requires free API key).
- Cargo type is derived from vessel type / tag (Tanker → Oil/LNG, etc.). Exact cargo manifests are not public.

## Next Steps

- Plug in aisstream.io for live AIS
- Add search by name / MMSI
- Historical track lines
- More sophisticated distress detection (AIS-SART, etc.)

Built as a companion to the Missile & Splashdown Tracker.
