"""Cloud Connector: агрегація даних з Edge, довготривале зберігання (InfluxDB bucket "cloud"), REST API."""
import json
import os
import signal
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from flask import Flask, jsonify, request

BROKER, PORT = os.getenv("MQTT_BROKER", "mqtt-broker"), int(os.getenv("MQTT_PORT", "1883"))
INFLUX_URL = os.getenv("INFLUXDB_URL", "")
CLOUD_BUCKET = os.getenv("INFLUXDB_CLOUD_BUCKET", "cloud")
ORG = os.getenv("INFLUXDB_ORG", "factory")
app = Flask(__name__)


class CloudConnector:
    def __init__(self):
        self.seen = set()                              # дедуплікація за id (at-least-once -> exactly-once ефект)
        self.dups = 0
        self.agg = defaultdict(lambda: deque(maxlen=500))   # (line,sensor) -> останні агрегати
        self.alerts = deque(maxlen=200)
        self.received = {"aggregates": 0, "alerts": 0}
        self.latency_ms = deque(maxlen=500)            # затримка Edge->Cloud (по часу вікна/алерта)
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="cloud-connector")
        self.client.will_set("cloud/status", "offline", qos=1, retain=True)   # LWT: брокер сам повідомить Edge
        self.client.on_connect, self.client.on_message = self.on_connect, self.on_message
        self.write_api = None
        if INFLUX_URL:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS
            self.influx = InfluxDBClient(url=INFLUX_URL, token=os.getenv("INFLUXDB_TOKEN"), org=ORG)
            self._ensure_bucket()
            self.write_api = self.influx.write_api(write_options=SYNCHRONOUS)

    def _ensure_bucket(self):
        for _ in range(30):
            try:
                api = self.influx.buckets_api()
                if not api.find_bucket_by_name(CLOUD_BUCKET):
                    api.create_bucket(bucket_name=CLOUD_BUCKET, org=ORG)
                return
            except Exception as e:
                print("[CLOUD] waiting for InfluxDB:", e, flush=True); time.sleep(2)

    def on_connect(self, client, userdata, flags, rc, props=None):
        client.subscribe([("cloud/aggregates", 1), ("cloud/alerts", 1)])
        client.publish("cloud/status", "online", qos=1, retain=True)
        print("[CLOUD] online", flush=True)

    def on_message(self, client, userdata, msg):
        try:
            d = json.loads(msg.payload)
            if d["id"] in self.seen:
                self.dups += 1; return
            self.seen.add(d["id"])
            if msg.topic == "cloud/aggregates":
                self.agg[(d["line"], d["sensor"])].append(d)
                self.received["aggregates"] += 1
                self._write_agg(d)
            else:
                self.alerts.append(d); self.received["alerts"] += 1
                self._write_alert(d)
        except Exception as e:
            print("[CLOUD] bad message:", e, flush=True)

    def _write_agg(self, d):
        if not self.write_api:
            return
        from influxdb_client import Point
        p = (Point("aggregates").tag("line", d["line"]).tag("sensor_id", d["sensor"])
             .field("mean", d["mean"]).field("min", float(d["min"])).field("max", float(d["max"]))
             .field("count", int(d["count"])).time(datetime.fromtimestamp(d["window_start"], timezone.utc)))
        self.write_api.write(bucket=CLOUD_BUCKET, record=p)   # той самий timestamp+теги -> ідемпотентний запис

    def _write_alert(self, d):
        if not self.write_api:
            return
        from influxdb_client import Point
        p = (Point("alerts").tag("line", d["line"]).tag("sensor_id", d["sensor"]).tag("level", d["level"])
             .field("value", float(d["value"])).field("rule", d["rule"]).time(datetime.fromisoformat(d["timestamp"])))
        self.write_api.write(bucket=CLOUD_BUCKET, record=p)

    def query_sensor(self, line, sensor, time_range):
        if self.write_api:  # історія з InfluxDB
            q = (f'from(bucket:"{CLOUD_BUCKET}") |> range(start: -{time_range}) '
                 f'|> filter(fn:(r)=> r._measurement=="aggregates" and r.line=="{line}" and r.sensor_id=="{sensor}" and r._field=="mean")')
            return [{"time": r.get_time().isoformat(), "mean": r.get_value()}
                    for t in self.influx.query_api().query(q) for r in t.records]
        return [{"time": datetime.fromtimestamp(a["window_start"], timezone.utc).isoformat(), "mean": a["mean"]}
                for a in self.agg[(line, sensor)]]

    def publish_command(self, cmd):
        self.client.publish("edge/commands", json.dumps(cmd), qos=1)

    def start(self):
        while True:
            try:
                self.client.connect(BROKER, PORT, keepalive=10); break
            except OSError as e:
                print("[CLOUD] broker unavailable:", e, flush=True); time.sleep(2)
        self.client.loop_start()

    def shutdown(self, *_):
        self.client.publish("cloud/status", "offline", qos=1, retain=True).wait_for_publish(2)
        self.client.loop_stop(); self.client.disconnect(); os._exit(0)


connector = CloudConnector()


@app.get("/api/health")
def health():
    return jsonify(status="ok", received=connector.received, duplicates_dropped=connector.dups)


@app.get("/api/sensors/<line>/<sensor>/data")
def sensor_data(line, sensor):
    return jsonify(connector.query_sensor(line, sensor, request.args.get("range", "1h")))


@app.get("/api/dashboard/summary")
def summary():
    latest = {f"{l}/{s}": q[-1] for (l, s), q in connector.agg.items() if q}
    levels = defaultdict(int)
    for a in connector.alerts:
        levels[a["level"]] += 1
    return jsonify(latest_aggregates=latest, alerts_by_level=levels, recent_alerts=list(connector.alerts)[-5:],
                   received=connector.received, duplicates_dropped=connector.dups)


@app.post("/api/commands")
def command():  # керування Edge з хмари, напр. {"command":"set_threshold","sensor":"temperature","key":"warn","value":65}
    connector.publish_command(request.get_json(force=True)); return jsonify(sent=True)


if __name__ == "__main__":
    connector.start()
    signal.signal(signal.SIGTERM, connector.shutdown)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), threaded=True)
