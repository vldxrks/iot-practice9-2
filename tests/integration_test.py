"""Інтеграційний тест Edge/Cloud БЕЗ Docker: локальний MQTT-брокер (amqtt) + реальні simulator/edge/connector.
Сценарії: 1) нормальна робота, 2) аварія хмари (SIGKILL) -> автономність Edge + буферизація, 3) відновлення -> синхронізація без дублів.
Запуск: python tests/integration_test.py  (потрібно: pip install amqtt paho-mqtt flask influxdb-client)"""
import json, os, signal, subprocess, sys, tempfile, time, urllib.request
import paho.mqtt.client as mqtt

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
PORT, API = 18830, 5055
tmp = tempfile.mkdtemp()
env = {**os.environ, "MQTT_BROKER": "127.0.0.1", "MQTT_PORT": str(PORT), "INFLUXDB_URL": "", "PORT": str(API),
       "SENSOR_INTERVAL": "0.2", "FAULT_PROB": "0.01", "SEED": "7", "BUFFER_DB": f"{tmp}/buf.db", "ALERT_COOLDOWN": "5"}
procs, logs = {}, {}


def start(name, cmd, cwd):
    logs[name] = open(f"{tmp}/{name}.log", "w")
    procs[name] = subprocess.Popen([sys.executable, "-u", *cmd], cwd=os.path.join(ROOT, cwd), env=env, stdout=logs[name], stderr=subprocess.STDOUT)


def api(path):
    return json.load(urllib.request.urlopen(f"http://127.0.0.1:{API}{path}", timeout=3))


metrics, events = [], []
def on_msg(c, u, m):
    if m.topic == "edge/metrics":
        metrics.append(json.loads(m.payload))
    events.append((time.time(), m.topic, m.payload.decode()[:80]))

open(f"{tmp}/broker.yaml", "w").write(f"""listeners:
  default:
    type: tcp
    bind: 127.0.0.1:{PORT}
plugins:
  amqtt.plugins.authentication.AnonymousAuthPlugin:
    allow_anonymous: true
""")
broker = subprocess.Popen(["amqtt", "-c", f"{tmp}/broker.yaml"], stdout=open(f"{tmp}/broker.log", "w"), stderr=subprocess.STDOUT)
time.sleep(3)
spy = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="spy"); spy.on_message = on_msg
spy.connect("127.0.0.1", PORT); spy.subscribe([("edge/metrics", 0), ("cloud/status", 1), ("actuators/#", 0)]); spy.loop_start()
report = {}
try:
    start("cloud", ["connector.py"], "cloud"); time.sleep(3)
    start("edge", ["processor.py"], "edge"); start("sensors", ["simulator.py"], "sensors")
    # --- 1. нормальна робота
    time.sleep(35)
    h = api("/api/health"); report["phase1_received"] = h["received"]
    report["phase1_alerts"] = api("/api/dashboard/summary")["alerts_by_level"]
    # --- 2. аварія хмари
    t_kill = time.time(); procs["cloud"].send_signal(signal.SIGKILL); procs["cloud"].wait()
    time.sleep(35)
    last = metrics[-1]
    report["phase2_edge_processed_during_outage"] = last["processed"]
    report["phase2_buffered_now"] = last["buffered_now"]
    report["phase2_edge_alive"] = procs["edge"].poll() is None
    report["phase2_actuator_cmds"] = sum(1 for e in events if e[1].startswith("actuators/"))
    # --- 3. відновлення
    t_restart = time.time(); start("cloud", ["connector.py"], "cloud"); time.sleep(25)
    h2 = api("/api/health"); sm = api("/api/dashboard/summary")
    report["phase3_received_after_restart"] = h2["received"]; report["phase3_dups_dropped"] = h2["duplicates_dropped"]
    report["phase3_buffered_left"] = metrics[-1]["buffered_now"]; report["phase3_synced_total"] = metrics[-1]["synced"]
    data = api("/api/sensors/line1/temperature/data")
    starts = sorted(int(__import__("datetime").datetime.fromisoformat(d["time"]).timestamp()) for d in data)
    gaps = [b - a for a, b in zip(starts, starts[1:]) if b - a != 10]
    report["phase3_windows"] = len(starts); report["phase3_gaps_in_windows"] = len(gaps)
    report["phase3_span_s"] = starts[-1] - starts[0] if starts else 0
    st = [e for e in events if e[1] == "cloud/status"]; report["cloud_status_events"] = [e[2] for e in st]
    lat = [m["p95_ms"] for m in metrics]; mean = [m["mean_ms"] for m in metrics]
    report["edge_latency_mean_ms(avg of reports)"] = round(sum(mean) / len(mean), 3)
    report["edge_latency_p95_ms(max)"] = max(lat); report["edge_latency_max_ms"] = max(m["max_ms"] for m in metrics)
    report["total_processed"] = metrics[-1]["processed"]; report["edge_errors"] = metrics[-1]["errors"]
finally:
    for p in list(procs.values()) + [broker]:
        try: p.send_signal(signal.SIGTERM); p.wait(5)
        except Exception: p.kill()
    spy.loop_stop()
print(json.dumps(report, indent=1))
json.dump(report, open(os.path.join(ROOT, "data", "integration_report.json"), "w"), indent=1)
os.system(f"cp {tmp}/edge.log {ROOT}/data/edge_run.log; cp {tmp}/sensors.log {ROOT}/data/sensors_run.log; cp {tmp}/cloud.log {ROOT}/data/cloud_run.log")
