#!/usr/bin/env python3
"""
MQTT Sensor Simulator
=====================
Publishes realistic drifting sensor data for 25 virtual devices at 1 Hz.

Topic  : mqttsim/dev/{devEUI}
Payload: {
            "deveui"    : "70B3D57ED0000001",
            "timestamp" : "2026-03-15T12:34:56Z",
            "sensors"   : {
                "temperature" : 21.4,
                "humidity"    : 43.2,
                ...              (2-5 fields, varies per device)
            }
         }

Install dependency:  pip install paho-mqtt
"""

import json
import time
import random
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# ── Configuration ─────────────────────────────────────────────────────────────
BROKER_HOST  = "cloud.sensor-online.se"
BROKER_PORT  = 3010
BROKER_USER  = "ellenex"
BROKER_PASS  = "Ellenex2022!"
TOPIC_PREFIX = "mqttsim/dev"
INTERVAL     = 1.0          # seconds between full publish rounds
QOS          = 0

# ── Exact devEUIs ─────────────────────────────────────────────────────────────
DEV_EUIS = [
    "70B3D57ED0000001",
    "70B3D57ED0000002",
    "70B3D57ED0000003",
    "70B3D57ED0000004",
    "70B3D57ED0000005",
    "70B3D57ED0000006",
    "70B3D57ED0000007",
    "70B3D57ED0000008",
    "70B3D57ED0000009",
    "70B3D57ED000000A",
    "70B3D57ED000000B",
    "70B3D57ED000000C",
    "70B3D57ED000000D",
    "70B3D57ED000000E",
    "70B3D57ED000000F",
    "70B3D57ED0000010",
    "70B3D57ED0000011",
    "70B3D57ED0000012",
    "70B3D57ED0000013",
    "70B3D57ED0000014",
    "70B3D57ED0000015",
    "70B3D57ED0000016",
    "70B3D57ED0000017",
    "70B3D57ED0000018",
    "70B3D57ED0000019",
]

# ── Sensor channel catalogue ───────────────────────────────────────────────────
#   min/max  = physical range
#   drift    = gaussian std-dev per step (realistic slow walk)
#   dec      = decimal places
CHANNELS = {
    "temperature":  {"min": -10.0,  "max":  50.0,  "drift": 0.30, "dec": 2},
    "humidity":     {"min":  20.0,  "max":  95.0,  "drift": 0.50, "dec": 1},
    "pressure":     {"min": 950.0,  "max":1050.0,  "drift": 0.20, "dec": 1},
    "co2":          {"min": 350.0,  "max":2000.0,  "drift":10.00, "dec": 0},
    "battery":      {"min":   2.8,  "max":   4.2,  "drift": 0.01, "dec": 3},
    "light":        {"min":   0.0,  "max":2000.0,  "drift":20.00, "dec": 0},
    "voltage":      {"min": 218.0,  "max": 242.0,  "drift": 0.50, "dec": 2},
    "current":      {"min":   0.0,  "max":  16.0,  "drift": 0.10, "dec": 3},
    "motion_count": {"min":   0.0,  "max": 500.0,  "drift": 2.00, "dec": 0},
    "soil_moisture":{"min":   0.0,  "max": 100.0,  "drift": 0.20, "dec": 1},
    "level":{"min":   0,  "max": 6,  "drift": 0.20, "dec": 1},
}

ALL_CHANNELS = list(CHANNELS.keys())

# ── Build sensor list with stable random channel assignments ───────────────────
random.seed(42)   # same channel set every run

sensors = []
for dev_eui in DEV_EUIS:
    n_ch     = random.randint(2, 5)
    channels = random.sample(ALL_CHANNELS, n_ch)

    # Start values in the comfortable middle of the range
    state = {}
    for ch in channels:
        cfg = CHANNELS[ch]
        state[ch] = random.uniform(
            cfg["min"] + (cfg["max"] - cfg["min"]) * 0.25,
            cfg["min"] + (cfg["max"] - cfg["min"]) * 0.75,
        )

    sensors.append({"devEUI": dev_eui, "channels": channels, "state": state})

# ── Drift one channel value by one step ───────────────────────────────────────
def drift(channel: str, current: float) -> float:
    cfg = CHANNELS[channel]
    if channel == "motion_count":
        new = current + random.randint(-3, 5)
    else:
        new = current + random.gauss(0, cfg["drift"])
    new = max(cfg["min"], min(cfg["max"], new))   # clamp to physical range
    return round(new, cfg["dec"])

# ── MQTT setup ─────────────────────────────────────────────────────────────────
client = mqtt.Client()

if BROKER_USER:
    client.username_pw_set(BROKER_USER, BROKER_PASS)

def on_connect(c, userdata, flags, rc):
    codes = {0:"OK", 1:"bad protocol", 2:"bad client id",
             3:"unavailable", 4:"bad credentials", 5:"not authorised"}
    print(f"MQTT connect: {codes.get(rc, rc)}")

def on_disconnect(c, userdata, rc):
    if rc != 0:
        print(f"Unexpected disconnect (rc={rc}), reconnecting …")

client.on_connect    = on_connect
client.on_disconnect = on_disconnect

print(f"Connecting to {BROKER_HOST}:{BROKER_PORT} …")
client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
client.loop_start()
time.sleep(0.5)   # let on_connect fire

# ── Print sensor overview ──────────────────────────────────────────────────────
print(f"\n{'devEUI':<20} channels")
print("-" * 65)
for s in sensors:
    print(f"  {s['devEUI']}  {', '.join(s['channels'])}")
print(f"\nPublishing {len(sensors)} devices to {TOPIC_PREFIX}/{{devEUI}} every {INTERVAL} s")
print("Ctrl+C to stop\n")

# ── Main publish loop ──────────────────────────────────────────────────────────
cycle = 0
try:
    while True:
        t0     = time.monotonic()
        cycle += 1
        now    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for sensor in sensors:
            sensor_values = {}
            for ch in sensor["channels"]:
                sensor["state"][ch] = drift(ch, sensor["state"][ch])
                sensor_values[ch]   = sensor["state"][ch]

            payload = {
                "deveui":    sensor["devEUI"],
                "timestamp": now,
                "sensors":   sensor_values,
            }

            topic = f"{TOPIC_PREFIX}/{sensor['devEUI']}"
            client.publish(topic, json.dumps(payload), qos=QOS)

        elapsed_ms = (time.monotonic() - t0) * 1000
        print(f"Cycle {cycle:5d} | {len(sensors)} msgs | {elapsed_ms:5.1f} ms | {now}")

        sleep_for = INTERVAL - (time.monotonic() - t0)
        if sleep_for > 0:
            time.sleep(sleep_for)

except KeyboardInterrupt:
    print("\nStopped by user.")
    client.loop_stop()
    client.disconnect()
