"""Edge Processor: локальна обробка в реальному часі, автономність без хмари, буферизація та синхронізація."""
import json
import os
import signal
import sqlite3  # noqa: F401
import threading
import time
from collections import deque
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from logic import Aggregator, EwmaAnomalyDetector, OfflineBuffer, RulesEngine

CFG = json.load(open(os.getenv("EDGE_CONFIG", "config.json")))
BROKER, PORT = os.getenv("MQTT_BROKER", "mqtt-broker"), int(os.getenv("MQTT_PORT", "1883"))
INFLUX_URL = os.getenv("INFLUXDB_URL", "")           # порожній -> локальний Influx вимкнено (тести без Docker)
ALERT_COOLDOWN = float(os.getenv("ALERT_COOLDOWN", "10"))


class EdgeProcessor:
    def __init__(self):
        self.rules = RulesEngine(CFG["rules"])
        self.detector = EwmaAnomalyDetector(**CFG["anomaly"])
        self.agg = Aggregator(CFG["window_seconds"])
        self.buffer = OfflineBuffer(os.getenv("BUFFER_DB", "/data/edge_buffer.db"), CFG["buffer_max_rows"])
        self.cloud_online, self.last_heartbeat = False, 0.0
        self.last_alert = {}
        self.lat = deque(maxlen=2000)
        self.stats = {"processed": 0, "alerts": 0, "anomalies": 0, "buffered": 0, "synced": 0, "errors": 0}
        self.stop = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="edge-processor")
        self.client.on_connect, self.client.on_message = self.on_connect, self.on_message
        self.write_api = self._init_influx()

    def _init_influx(self):
        if not INFLUX_URL:
            return None
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import WriteOptions
        self.influx = InfluxDBClient(url=INFLUX_URL, token=os.getenv("INFLUXDB_TOKEN"), org=os.getenv("INFLUXDB_ORG", "factory"))
        return self.influx.write_api(write_options=WriteOptions(batch_size=100, flush_interval=2000))  # batch-запис

    # ---- MQTT ----
    def on_connect(self, client, userdata, flags, rc, props=None):
        client.subscribe([("sensors/#", 0), ("cloud/status", 1), ("edge/commands", 1)])
        print("[EDGE] connected, subscribed", flush=True)

    def on_message(self, client, userdata, msg):
        try:
            if msg.topic == "cloud/status":
                self.cloud_online = msg.payload.decode() == "online"; self.last_heartbeat = time.time()
                print(f"[EDGE] cloud status: {'ONLINE' if self.cloud_online else 'OFFLINE'}", flush=True)
            elif msg.topic == "edge/commands":
                self.handle_command(json.loads(msg.payload))
            else:
                self.process(json.loads(msg.payload))
        except Exception as e:
            self.stats["errors"] += 1; print("[EDGE] error:", e, flush=True)

    def handle_command(self, cmd):
        if cmd.get("command") == "set_threshold":
            self.rules.set_threshold(cmd["sensor"], cmd["key"], cmd["value"])
            print(f"[EDGE] threshold updated from cloud: {cmd}", flush=True)

    # ---- обробка вимірів (< 100 мс) ----
    def process(self, d):
        t0 = time.perf_counter()
        line, sensor, value = d["line"], d["sensor_id"], float(d["value"])
        if value != value:  # NaN
            return
        self.agg.add(line, sensor, value, time.time())
        z, anomaly = self.detector.update((line, sensor), value)
        verdict = self.rules.evaluate(sensor, value)
        if verdict or anomaly:
            self.raise_alert(line, sensor, value, verdict, z, anomaly)
        self.store_local(d)
        self.stats["processed"] += 1
        self.lat.append((time.perf_counter() - t0) * 1000)

    def raise_alert(self, line, sensor, value, verdict, z, anomaly):
        level = verdict["level"] if verdict else "anomaly"
        key = (line, sensor, level)
        now = time.time()
        if now - self.last_alert.get(key, 0) < ALERT_COOLDOWN:     # анти-спам
            return
        self.last_alert[key] = now
        alert = {"id": f"alert:{line}:{sensor}:{int(now*1000)}", "line": line, "sensor": sensor, "value": value,
                 "level": level, "rule": verdict["rule"] if verdict else f"ewma z={z:.1f}",
                 "timestamp": datetime.now(timezone.utc).isoformat()}
        self.stats["alerts"] += 1; self.stats["anomalies"] += int(anomaly)
        print(f"[EDGE] ALERT {level} {line}/{sensor}={value} ({alert['rule']})", flush=True)
        if verdict and verdict["level"] == "critical" and verdict["action"]:       # локальна автоматична реакція
            self.client.publish(f"actuators/{line}/{verdict['action']}",
                                json.dumps({"command": verdict["action"], "reason": alert["rule"],
                                            "timestamp": alert["timestamp"]}), qos=1)
        self.send_to_cloud("cloud/alerts", alert["id"], alert)                        # critical без затримки

    def store_local(self, d):
        if not self.write_api:
            return
        from influxdb_client import Point
        p = (Point("sensor_data").tag("line", d["line"]).tag("sensor_id", d["sensor_id"])
             .field("value", float(d["value"])).time(datetime.fromisoformat(d["timestamp"])))
        self.write_api.write(bucket=os.getenv("INFLUXDB_EDGE_BUCKET", "edge"), record=p)

    # ---- синхронізація з хмарою ----
    def send_to_cloud(self, topic, msg_id, payload):
        if self.cloud_online:
            self.client.publish(topic, json.dumps(payload), qos=1)
        else:
            self.buffer.add(msg_id, topic, payload, time.time()); self.stats["buffered"] += 1

    def flush_loop(self):
        while not self.stop.is_set():
            time.sleep(1)
            if time.time() - self.last_heartbeat > CFG["heartbeat_timeout_seconds"] * 6 and self.cloud_online:
                pass  # статус тримає retained+LWT повідомлення брокера (cloud/status)
            for agg in self.agg.flush(time.time()):
                self.send_to_cloud("cloud/aggregates", agg["id"], agg)
            if self.cloud_online and self.buffer.count():
                batch = self.buffer.pop_batch(200)
                for mid, topic, payload in batch:
                    self.client.publish(topic, payload, qos=1)
                self.buffer.delete([b[0] for b in batch]); self.stats["synced"] += len(batch)
                print(f"[EDGE] synced {len(batch)} buffered msgs, left={self.buffer.count()}", flush=True)
            if int(time.time()) % 10 == 0 and self.lat:
                s = sorted(self.lat)
                m = {"processed": self.stats["processed"], "mean_ms": round(sum(s)/len(s), 3),
                     "p95_ms": round(s[int(len(s)*.95)-1], 3), "max_ms": round(s[-1], 3),
                     "buffered_now": self.buffer.count(), **{k: v for k, v in self.stats.items() if k != "processed"}}
                self.client.publish("edge/metrics", json.dumps(m), qos=0)
                print("[EDGE] metrics", m, flush=True)

    def run(self):
        while not self.stop.is_set():
            try:
                self.client.connect(BROKER, PORT, keepalive=30); break
            except OSError as e:
                print("[EDGE] broker unavailable, retry:", e, flush=True); time.sleep(2)
        threading.Thread(target=self.flush_loop, daemon=True).start()
        self.client.loop_start()
        while not self.stop.is_set():
            time.sleep(0.5)
        self.client.loop_stop(); self.client.disconnect()
        if self.write_api:
            self.write_api.close()
        print("[EDGE] graceful shutdown", flush=True)


if __name__ == "__main__":
    p = EdgeProcessor()
    signal.signal(signal.SIGTERM, lambda *a: p.stop.set()); signal.signal(signal.SIGINT, lambda *a: p.stop.set())
    p.run()
