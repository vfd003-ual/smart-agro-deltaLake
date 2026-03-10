import asyncio
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from confluent_kafka import Consumer

# --- 1. THE FRONTEND (HTML + JS + Leaflet Map) ---
# We store the HTML as a string so everything fits in one script!
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>Live ISS Tracker (Kafka + WebSockets)</title>
    <!-- Load Leaflet Map CSS and JS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body { margin: 0; padding: 0; font-family: sans-serif; }
        #map { height: 100vh; width: 100vw; }
        #overlay {
            position: absolute; top: 10px; left: 50px; z-index: 1000;
            background: white; padding: 10px 20px; border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.3);
        }
    </style>
</head>
<body>
    <div id="overlay">
        <h3 style="margin: 0 0 5px 0;">🛰️ Live ISS Location</h3>
        <div id="coords">Waiting for Kafka...</div>
        <div id="last-update" style="font-size: 0.9em; margin-top: 5px; color: #666;"></div>
    </div>
    <div id="map"></div>

    <script>
        // Initialize the map, centered at [0,0] with zoom level 3
        var map = L.map('map').setView([0, 0], 3);
        
        // Load OpenStreetMap tiles
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 19,
            attribution: '© OpenStreetMap'
        }).addTo(map);

        // Create a custom ISS Icon
        var issIcon = L.icon({
            iconUrl: 'https://upload.wikimedia.org/wikipedia/commons/d/d0/International_Space_Station.svg',
            iconSize: [60, 40]
        });

        // Add the marker and trailing red line segments
        var marker = L.marker([0, 0], {icon: issIcon}).addTo(map);
        var pathGroup = L.layerGroup().addTo(map);
        var currentPath = L.polyline([], {color: 'red', weight: 3});
        pathGroup.addLayer(currentPath);
        var lastLon = null;

        // Connect back to FastAPI via WebSockets
        var ws = new WebSocket(`ws://${window.location.host}/ws`);

        ws.onmessage = function(event) {
            var data = JSON.parse(event.data);
            var lat = parseFloat(data.iss_position.latitude);
            var lon = parseFloat(data.iss_position.longitude);
            var timestamp = new Date(data.timestamp * 1000).toLocaleString();

            // Update the UI text
            document.getElementById('coords').innerText = `Lat: ${lat} | Lon: ${lon}`;
            document.getElementById('last-update').innerText = `Last Update: ${timestamp}`;
            
            var newLatLng = new L.LatLng(lat, lon);

            // If the ISS crossed the antimeridian, start a new polyline segment
            if (lastLon !== null && Math.abs(lon - lastLon) > 180) {
                currentPath = L.polyline([], {color: 'red', weight: 3});
                pathGroup.addLayer(currentPath);
            }
            lastLon = lon;
            
            // Move the marker, draw the trail, and pan the map
            marker.setLatLng(newLatLng);
            currentPath.addLatLng(newLatLng);
            map.panTo(newLatLng);
        };
    </script>
</body>
</html>
"""

# Store connected web browsers
active_connections = set()

# --- 2. THE BACKGROUND KAFKA CONSUMER ---
async def consume_kafka_to_websockets():
    """ Runs in the background, reading Kafka and pushing to WebSockets. """
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'iss-website-group',
        # 'latest' means we only care about the current position when the server starts
        'auto.offset.reset': 'latest' 
    }
    consumer = Consumer(conf)
    consumer.subscribe(['iss_telemetry'])

    print("🎧 Background Kafka Consumer started. Waiting for ISS data...")
    
    try:
        while True:
            # We use asyncio.to_thread so the blocking Kafka poll() doesn't freeze the web server
            msg = await asyncio.to_thread(consumer.poll, 0.5)
            
            if msg is not None and not msg.error():
                data = msg.value().decode('utf-8')
                
                # Broadcast the new coordinates to every open browser tab
                disconnected = set()
                for ws in active_connections:
                    try:
                        await ws.send_text(data)
                    except Exception:
                        disconnected.add(ws)
                
                # Clean up closed tabs
                for ws in disconnected:
                    active_connections.remove(ws)
    finally:
        consumer.close()

# --- 3. THE WEB SERVER (FastAPI) ---
# Lifespan starts the Kafka background task when the server turns on
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_kafka_to_websockets())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def get_webpage():
    """ Serves the map HTML """
    return HTMLResponse(HTML_PAGE)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """ Handles new browser connections """
    await websocket.accept()
    active_connections.add(websocket)
    try:
        # Keep the connection open indefinitely
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)

# Entry point for running the server programmatically
if __name__ == "__main__":
    import uvicorn
    print("🌍 Starting Web Server on http://localhost:7777")
    uvicorn.run("consumer_web:app", host="0.0.0.0", port=7777, reload=True)