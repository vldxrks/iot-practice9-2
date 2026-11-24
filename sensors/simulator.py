"""Симулятор датчиків виробничих ліній (практична 9.2, варіант 1: Smart Factory).

Публікує JSON у топіки sensors/<line>/<sensor>. Підписаний на actuators/<line>/#:
команда охолодження реально знижує температуру (замкнений контур керування).
"""
import json
import math
import os
import random
import signal
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

BROKER = os.getenv("MQTT_BROKER", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
INTERVAL = float(os.getenv("SENSOR_INTERVAL", "1.0"))        # секунд між вимірами
LINES = os.getenv("LINES", "line1,line2").split(",")
FAULT_PROB = float(os.getenv("FAULT_PROB", "0.004"))         # ймовірність початку несправності на тік
SEED = os.getenv("SEED")

# base / amp (добова синусоїда навантаження) / noise / unit
SENSORS = {
    "temperature":   dict(base=55.0, amp=6.0,  noise=0.6,  unit="C"),
    "vibration":     dict(base=2.2,  amp=0.4,  noise=0.12, unit="mm/s"),
    "motor_current": dict(base=24.0, amp=4.0,  noise=0.7,  unit="A"),
    "pressure":      dict(base=6.0,  amp=0.3,  noise=0.05, unit="bar"),
    "conveyor_speed": dict(base=1.2, amp=0.1,  noise=0.02, unit="m/s"),
}
FAULTS = {  # тип несправності -> (датчик, множник/зсув, тривалість тікс)
    "overheat":       ("temperature", +28.0, (15, 40)),
    "bearing_wear":   ("vibration", +4.0, (20, 50)),
    "overcurrent":    ("motor_current", +16.0, (10, 30)),
    "pressure_drop":  ("pressure", -2.2, (15, 40)),
}


class Simulator:
    def __init__(self):
        if SEED:
            random.seed(int(SEED))
        self.running = threading.Event(); self.running.set()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="sensor-simulator")
        self.client.on_message = self.on_message
        self.client.on_connect = lambda c, u, f, rc, p=None: c.subscribe("actuators/#")
        self.cooling = {l: 0 for l in LINES}        # залишок тікс дії охолодження
        self.faults = {l: None for l in LINES}      # (kind, залишок, повна довжина)
        self.seq = 0

    def on_message(self, client, userdata, msg):
        try:
            line = msg.topic.split("/")[1]
            cmd = json.loads(msg.payload)
            if cmd.get("command") == "enable_cooling" and line in self.cooling:
                self.cooling[line] = 20
                print(f"[SIM] cooling ON for {line}", flush=True)
        except Exception as e:
            print("[SIM] bad actuator msg:", e, flush=True)

    def connect(self):
        while self.running.is_set():                # перепідключення, поки брокер не готовий
            try:
                self.client.connect(BROKER, PORT, keepalive=30)
                self.client.loop_start()
                print(f"[SIM] connected to {BROKER}:{PORT}", flush=True)
                return
            except OSError as e:
                print("[SIM] broker unavailable, retry in 2s:", e, flush=True); time.sleep(2)

    def value(self, line, name, cfg, tick):
        phase = (hash(line) % 7) / 7 * 2 * math.pi
        v = cfg["base"] + cfg["amp"] * math.sin(tick / 120 + phase) + random.gauss(0, cfg["noise"])
        f = self.faults[line]
        if f and FAULTS[f[0]][0] == name:           # лінійне наростання несправності
            kind, left, total = f
            v += FAULTS[kind][1] * (1 - left / total * 0.5)
        if name == "temperature" and self.cooling[line] > 0:
            v -= 12.0
        return v

    def step_faults(self, line):
        f = self.faults[line]
        if f:
            kind, left, total = f
            self.faults[line] = (kind, left - 1, total) if left > 1 else None
        elif random.random() < FAULT_PROB:
            kind = random.choice(list(FAULTS)); n = random.randint(*FAULTS[kind][2])
            self.faults[line] = (kind, n, n)
            print(f"[SIM] FAULT {kind} on {line} for {n} ticks", flush=True)
        if self.cooling[line] > 0:
            self.cooling[line] -= 1

    def run(self):
        self.connect()
        tick = 0
        while self.running.is_set():
            for line in LINES:
                self.step_faults(line)
                for name, cfg in SENSORS.items():
                    self.seq += 1
                    payload = {"line": line, "sensor_id": name, "value": round(self.value(line, name, cfg, tick), 3),
                               "unit": cfg["unit"], "seq": self.seq, "fault": (self.faults[line][0] if self.faults[line] else None),
                               "timestamp": datetime.now(timezone.utc).isoformat()}
                    self.client.publish(f"sensors/{line}/{name}", json.dumps(payload), qos=0)
            tick += 1
            time.sleep(INTERVAL)

    def stop(self, *_):
        print("[SIM] shutting down", flush=True)
        self.running.clear()


if __name__ == "__main__":
    sim = Simulator()
    signal.signal(signal.SIGTERM, sim.stop); signal.signal(signal.SIGINT, sim.stop)
    sim.run()
    sim.client.loop_stop(); sim.client.disconnect()
