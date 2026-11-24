"""Чиста (без мережі) бізнес-логіка Edge: правила, виявлення аномалій, агрегація, офлайн-буфер.
Винесена окремо, щоб її можна було тестувати без MQTT/Docker (tests/test_logic.py)."""
import json
import math
import sqlite3
import threading
from collections import defaultdict


class RulesEngine:
    """Порогові правила з пріоритетами: critical > warning."""
    def __init__(self, rules):
        self.rules = rules

    def evaluate(self, sensor, value):
        r = self.rules.get(sensor)
        if not r:
            return None
        if "critical" in r and value >= r["critical"]:
            return {"level": "critical", "action": r["action"], "rule": f"{sensor}>={r['critical']}"}
        if "low_critical" in r and value <= r["low_critical"]:
            return {"level": "critical", "action": r["action"], "rule": f"{sensor}<={r['low_critical']}"}
        if "warn" in r and value >= r["warn"]:
            return {"level": "warning", "action": None, "rule": f"{sensor}>={r['warn']}"}
        if "low_warn" in r and value <= r["low_warn"]:
            return {"level": "warning", "action": None, "rule": f"{sensor}<={r['low_warn']}"}
        return None

    def set_threshold(self, sensor, key, value):
        self.rules.setdefault(sensor, {})[key] = float(value)


class EwmaAnomalyDetector:
    """Легка модель нормальної поведінки для Edge: EWMA середнє/дисперсія та z-score по кожному (line, sensor)."""
    def __init__(self, alpha=0.05, z_threshold=4.5, warmup=40):
        self.alpha, self.z, self.warmup = alpha, z_threshold, warmup
        self.state = {}

    def update(self, key, x):
        s = self.state.get(key)
        if s is None:
            self.state[key] = {"mean": x, "var": 1e-6, "n": 1}
            return 0.0, False
        std = math.sqrt(s["var"])
        z = abs(x - s["mean"]) / std if std > 1e-9 else 0.0
        anomaly = s["n"] >= self.warmup and z > self.z
        if not anomaly:  # аномальні точки не "отруюють" модель норми
            d = x - s["mean"]
            s["mean"] += self.alpha * d
            s["var"] = (1 - self.alpha) * (s["var"] + self.alpha * d * d)
        s["n"] += 1
        return z, anomaly


class Aggregator:
    """Вікна фіксованої довжини: count/min/max/mean на (line, sensor). Ідентифікатор вікна дає ідемпотентність."""
    def __init__(self, window_seconds=10):
        self.w = window_seconds
        self.acc = defaultdict(lambda: {"count": 0, "sum": 0.0, "min": float("inf"), "max": float("-inf")})

    def add(self, line, sensor, value, epoch):
        start = int(epoch // self.w * self.w)
        a = self.acc[(start, line, sensor)]
        a["count"] += 1; a["sum"] += value
        a["min"] = min(a["min"], value); a["max"] = max(a["max"], value)

    def flush(self, now_epoch):
        """Повертає лише ЗАВЕРШЕНІ вікна (початок + w <= now)."""
        done = []
        for (start, line, sensor) in [k for k in self.acc if k[0] + self.w <= now_epoch]:
            a = self.acc.pop((start, line, sensor))
            done.append({"id": f"{line}:{sensor}:{start}", "line": line, "sensor": sensor, "window_start": start,
                         "window_s": self.w, "count": a["count"], "mean": round(a["sum"] / a["count"], 4),
                         "min": a["min"], "max": a["max"]})
        return done


class OfflineBuffer:
    """Персистентний буфер повідомлень для хмари (SQLite). PRIMARY KEY(id) -> без дублікатів. Обмежений за розміром."""
    def __init__(self, path=":memory:", max_rows=50000):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock, self.max_rows = threading.Lock(), max_rows
        self.db.execute("CREATE TABLE IF NOT EXISTS pending (id TEXT PRIMARY KEY, topic TEXT, payload TEXT, created REAL)")
        self.db.commit()

    def add(self, msg_id, topic, payload, created):
        with self.lock:
            self.db.execute("INSERT OR IGNORE INTO pending VALUES (?,?,?,?)", (msg_id, topic, json.dumps(payload), created))
            self.db.execute("DELETE FROM pending WHERE id IN (SELECT id FROM pending ORDER BY created ASC "
                            "LIMIT max(0, (SELECT COUNT(*) FROM pending) - ?))", (self.max_rows,))
            self.db.commit()

    def pop_batch(self, n=200):
        with self.lock:
            return self.db.execute("SELECT id, topic, payload FROM pending ORDER BY created LIMIT ?", (n,)).fetchall()

    def delete(self, ids):
        with self.lock:
            self.db.executemany("DELETE FROM pending WHERE id=?", [(i,) for i in ids])
            self.db.commit()

    def count(self):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM pending").fetchone()[0]
