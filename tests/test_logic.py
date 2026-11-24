"""Модульні тести логіки Edge (без Docker/MQTT): python tests/test_logic.py"""
import json, os, random, sys, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "edge"))
from logic import Aggregator, EwmaAnomalyDetector, OfflineBuffer, RulesEngine

RULES = json.load(open(os.path.join(os.path.dirname(__file__), "..", "edge", "config.json")))["rules"]


def test_rules():
    r = RulesEngine(json.loads(json.dumps(RULES)))
    assert r.evaluate("temperature", 55) is None
    assert r.evaluate("temperature", 72)["level"] == "warning"
    v = r.evaluate("temperature", 80)
    assert v["level"] == "critical" and v["action"] == "enable_cooling"
    assert r.evaluate("pressure", 3.5)["level"] == "critical"      # нижній поріг
    r.set_threshold("temperature", "warn", 50)                      # команда з хмари
    assert r.evaluate("temperature", 55)["level"] == "warning"


def test_anomaly_detector():
    random.seed(1); d = EwmaAnomalyDetector(0.05, 4.5, 40)
    flags = [d.update("k", 55 + random.gauss(0, 0.6))[1] for _ in range(300)]
    assert sum(flags) <= 2                                           # майже немає хибних спрацювань на нормі
    assert d.update("k", 80)[1] is True                              # різкий стрибок виявлено
    assert d.update("k", 55.2)[1] is False                           # модель не "отруєна" аномалією


def test_aggregator_closed_windows_only():
    a = Aggregator(10)
    for i, v in enumerate([1, 2, 3, 4]):
        a.add("l1", "t", v, 100 + i)
    assert a.flush(105) == []                                        # вікно ще відкрите
    out = a.flush(110)
    assert len(out) == 1 and out[0]["count"] == 4 and out[0]["mean"] == 2.5 and out[0]["id"] == "l1:t:100"
    assert a.flush(200) == []                                        # вікно не віддається двічі


def test_offline_buffer_dedup_persist_limit():
    path = os.path.join(tempfile.mkdtemp(), "b.db")
    b = OfflineBuffer(path, max_rows=5)
    for i in range(3):
        b.add("id1", "cloud/aggregates", {"x": i}, i)                # однаковий id -> один запис
    assert b.count() == 1
    b2 = OfflineBuffer(path, max_rows=5); assert b2.count() == 1     # переживає "перезапуск"
    for i in range(10):
        b2.add(f"m{i}", "t", {}, 100 + i)
    assert b2.count() == 5                                           # обмеження розміру (видаляє найстаріші)
    ids = [r[0] for r in b2.pop_batch(10)]; b2.delete(ids); assert b2.count() == 0


if __name__ == "__main__":
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            f(); print("PASS", n)
