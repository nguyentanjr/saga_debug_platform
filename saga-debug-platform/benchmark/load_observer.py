"""B2 — benchmark tai cua observer: event/giay chiu duoc, do tre khi tai tang, diem bao hoa.

Bom vao Kafka cung topic, khoa (sagaId) va payload ma order/payment/inventory-service phat
ra, theo tung bac toc do. Moi saga di dung workflow ORDER_SAGA: 90% luong thanh cong
(3 event), 10% luong bu tru (5 event), cac event cach nhau --gap giay -- nen o toc do R
co khoang R/3.2 saga moi moi giay va R*gap/3.2*(so buoc) saga dang bay cung luc.

Moi bac do:
  offered      toc do da gui that (event/s)
  ingested     event observer ghi vao saga_events TRONG cua so bac do / thoi luong bac
  latency      received_at (observer nhan, dong ho VM Docker) - timestamp trong payload (luc
               gui, dong ho host) da hieu chinh lech dong ho: p50 / p95 / p99 / max.
               Tren Docker Desktop hai dong ho troi nhau toi ~100 ms/giay, nen ClockSync
               hoi gio VM qua PostgreSQL moi 50 ms va noi suy offset (sai so <= RTT/2).
  lag_end      consumer lag (correlation-engine-group) luc ket thuc bac
  drain_s      thoi gian tu luc ngung gui den khi lag ve 0
  lost         event da gui nhung khong co trong saga_events sau khi drain
  anomalies    bat thuong observer ghi cho saga cua bac nay (moi saga deu binh thuong,
               nen moi bat thuong la bao dong gia do tai)
  cpu / mem    docker stats cua observer va postgres (trung binh / dinh)

Chay (sau khi compose da len va observer UP):
  python benchmark/load_observer.py --rates 100,500,1000,2000,5000 --duration 60
"""
import argparse
import bisect
import heapq
import json
import os
import random
import statistics
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:39092"
GROUP = "correlation-engine-group"
PG = dict(host="localhost", port=35432, user="demo", password="demo", dbname="correlation_db")
TOPICS = ["order-created", "payment-completed", "inventory-reserved", "inventory-failed",
          "refund-payment", "payment-refunded"]
HAPPY = [("order-created", "ORDER_CREATED", "CREATE_ORDER", "order-service"),
         ("payment-completed", "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service"),
         ("inventory-reserved", "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service")]
COMPENSATE = HAPPY[:2] + [
    ("inventory-failed", "INVENTORY_FAILED", "RESERVE_INVENTORY", "inventory-service"),
    ("refund-payment", "REFUND_PAYMENT", "REFUND_PAYMENT", "order-service"),
    ("payment-refunded", "PAYMENT_REFUNDED", "REFUND_PAYMENT", "payment-service")]
MEAN_EVENTS = 0.9 * len(HAPPY) + 0.1 * len(COMPENSATE)


def ensure_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    have = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(t, num_partitions=3, replication_factor=1) for t in TOPICS if t not in have]
    for f in admin.create_topics(new).values() if new else []:
        f.result()


def total_lag(consumer, retries=5):
    for attempt in range(retries):
        try:
            return _total_lag(consumer)
        except Exception:  # leader/coordinator dang bau lai ngay sau khi tao topic
            time.sleep(1)
    return _total_lag(consumer)


def _total_lag(consumer):
    md = consumer.list_topics(timeout=10)
    parts = [TopicPartition(t, p) for t in TOPICS for p in md.topics[t].partitions]
    committed = consumer.committed(parts, timeout=10)
    lag = 0
    for tp in committed:
        lo, hi = consumer.get_watermark_offsets(tp, timeout=10)
        lag += hi - (tp.offset if tp.offset >= 0 else lo)
    return lag


class Stats(threading.Thread):
    """docker stats --no-stream lap lai cho observer va postgres."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples, self.stop = [], threading.Event()

    def run(self):
        while not self.stop.is_set():
            out = subprocess.run(
                ["docker", "stats", "--no-stream", "--format",
                 "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}", "bench-observer", "bench-postgres"],
                capture_output=True, text=True).stdout
            for line in out.strip().splitlines():
                name, cpu, mem = line.split("|")
                used = mem.split("/")[0].strip()
                mib = float(used[:-3]) * (1024 if used.endswith("GiB") else 1) \
                    if used[-3:] in ("GiB", "MiB") else 0.0
                self.samples.append((name, float(cpu.rstrip("%")), mib))

    def summary(self):
        out = {}
        for name in ("bench-observer", "bench-postgres"):
            cpu = [c for n, c, _ in self.samples if n == name]
            mem = [m for n, _, m in self.samples if n == name]
            if cpu:
                out[name] = {"cpu_avg_pct": round(statistics.mean(cpu), 1),
                             "cpu_max_pct": round(max(cpu), 1),
                             "mem_max_mib": round(max(mem), 1)}
        return out


class ClockSync(threading.Thread):
    """Offset (gio VM - gio host) theo thoi gian, do bang SELECT clock_timestamp()."""

    def __init__(self):
        super().__init__(daemon=True)
        self.points, self.stop = [], threading.Event()  # (host_mid, offset_s, rtt_s)
        self.lock = threading.Lock()

    def run(self):
        with psycopg2.connect(**PG) as c, c.cursor() as cur:
            while not self.stop.is_set():
                t1 = time.time()
                cur.execute("SELECT EXTRACT(EPOCH FROM clock_timestamp())::float8")
                vm = cur.fetchone()[0]
                t2 = time.time()
                with self.lock:
                    self.points.append(((t1 + t2) / 2, vm - (t1 + t2) / 2, t2 - t1))
                time.sleep(0.05)

    def offset(self, t):
        with self.lock:
            pts = list(self.points)
        i = bisect.bisect_left(pts, (t,))
        if i == 0:
            return pts[0][1]
        if i == len(pts):
            return pts[-1][1]
        (ta, oa, _), (tb, ob, _) = pts[i - 1], pts[i]
        return oa + (ob - oa) * (t - ta) / (tb - ta)

    def steps(self, threshold=0.2):
        """Thoi diem (gio host) dong ho VM bi nhay bac > threshold giay giua hai mau."""
        with self.lock:
            pts = list(self.points)
        return [(a[0] + b[0]) / 2 for a, b in zip(pts, pts[1:]) if abs(b[1] - a[1]) > threshold]

    def summary(self):
        with self.lock:
            off = [o for _, o, _ in self.points]
            rtt = [r for _, _, r in self.points]
        return {"samples": len(off), "offset_min_ms": round(min(off) * 1000, 1),
                "offset_max_ms": round(max(off) * 1000, 1),
                "rtt_p95_ms": round(sorted(rtt)[int(0.95 * (len(rtt) - 1))] * 1000, 1)}


def payload(saga, step, ev, prev_id):
    topic, etype, step_name, service = ev
    body = {"sagaId": saga["id"], "orderId": saga["order"], "eventId": str(uuid.uuid4()),
            "eventType": etype, "stepName": step_name, "serviceName": service,
            "traceId": saga["trace"], "spanId": uuid.uuid4().hex[:16],
            "productId": "PRODUCT-1", "quantity": 1, "amount": 100.0,
            "timestamp": time.time(), "benchStep": step}
    if prev_id:
        body["causationId"] = prev_id
    return topic, body


def run_step(producer, consumer, rate, duration, gap, step_id):
    sagas_per_s = rate / MEAN_EVENTS
    heap, seq, sent, sagas = [], 0, 0, 0
    lag_samples = []
    t0 = time.time()
    next_saga, next_lag = t0, t0
    while True:
        now = time.time()
        if now - t0 < duration:
            while next_saga <= now:
                path = COMPENSATE if random.random() < 0.1 else HAPPY
                s = {"id": str(uuid.uuid4()), "order": str(uuid.uuid4()),
                     "trace": uuid.uuid4().hex, "path": path, "prev": None}
                for i in range(len(path)):
                    heapq.heappush(heap, (next_saga + i * gap, seq, s, i))
                    seq += 1
                sagas += 1
                next_saga += 1.0 / sagas_per_s
        elif not heap:
            break
        while heap and heap[0][0] <= now:
            _, _, s, i = heapq.heappop(heap)
            topic, body = payload(s, step_id, s["path"][i], s["prev"])
            s["prev"] = body["eventId"]
            while True:
                try:
                    producer.produce(topic, key=s["id"], value=json.dumps(body))
                    break
                except BufferError:
                    producer.poll(0.01)
            sent += 1
        producer.poll(0)
        if now >= next_lag:
            lag_samples.append((round(now - t0, 1), total_lag(consumer)))
            next_lag = now + 2.0
        time.sleep(0.001)
    producer.flush(30)
    t_end = time.time()
    return {"sent": sent, "sagas": sagas, "t0": t0, "t_end": t_end,
            "offered_eps": round(sent / (t_end - t0), 1), "lag_samples": lag_samples}


def drain(consumer, timeout):
    t = time.time()
    while time.time() - t < timeout:
        lag = total_lag(consumer)
        if lag == 0:
            return round(time.time() - t, 1), 0
        time.sleep(1)
    return None, total_lag(consumer)


def db_metrics(step_id, t0, t_end, clock):
    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("""SELECT EXTRACT(EPOCH FROM received_at)::float8,
                              (payload->>'timestamp')::float8
                       FROM saga_events WHERE payload->>'benchStep' = %s""", (step_id,))
        rows = cur.fetchall()
        cur.execute("""
            SELECT a.anomaly_type, count(*) FROM anomalies a
            WHERE a.saga_id IN (SELECT DISTINCT saga_id FROM saga_events
                                WHERE payload->>'benchStep' = %s)
            GROUP BY 1""", (step_id,))
        anomalies = dict(cur.fetchall())
        cur.execute("SELECT pg_database_size(current_database())")
        size = cur.fetchone()[0]
    end_vm = t_end + clock.offset(t_end)
    # Bo event co mot lan nhay bac dong ho VM nam giua luc gui va luc nhan: noi suy offset
    # qua mot buoc nhay cho sai so bang dung buoc nhay (~1-2 s), khong phai do tre.
    jumps = clock.steps()
    lat, skipped = [], 0
    for recv, sent in rows:
        recv_host = recv - clock.offset(sent)
        if any(sent - 0.1 <= j <= recv_host + 0.1 for j in jumps):
            skipped += 1
            continue
        lat.append((recv - (sent + clock.offset(sent))) * 1000)
    lat.sort()
    in_window = sum(1 for recv, _ in rows if recv <= end_vm)

    def pct(q):
        return _r(lat[int(q * (len(lat) - 1))]) if lat else None
    return {"stored": len(rows), "stored_within_step": in_window,
            "ingested_eps": round(in_window / (t_end - t0), 1),
            "latency_ms": {"p50": pct(0.5), "p95": pct(0.95), "p99": pct(0.99),
                           "min": pct(0.0), "max": pct(1.0),
                           "excluded_clock_jump": skipped},
            "anomalies": anomalies, "db_size_mib": round(size / 2**20, 1)}


def _r(x):
    return round(x, 1) if x is not None else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rates", default="100,500,1000,2000,5000")
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--gap", type=float, default=0.5, help="giay giua hai event cua mot saga")
    ap.add_argument("--drain-timeout", type=int, default=600)
    ap.add_argument("--settle", type=int, default=40,
                    help="cho them sau drain de detector theo lich (15s) quet xong")
    ap.add_argument("--out", default=None)
    ap.add_argument("--label", default="")
    a = ap.parse_args()

    ensure_topics()  # topic phai co truoc khi observer khoi dong: pattern subscription chi
                     # thay topic moi sau metadata.max.age.ms (5 phut)
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 5,
                         "queue.buffering.max.messages": 1_000_000, "acks": "1"})
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP,
                         "enable.auto.commit": False})
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = {"run_id": run_id, "label": a.label, "gap_s": a.gap, "duration_s": a.duration,
              "mean_events_per_saga": MEAN_EVENTS, "steps": []}

    clock = ClockSync()
    clock.start()
    time.sleep(2)

    # Khoi dong: consumer group da join va JIT da am truoc bac dau tien.
    run_step(producer, consumer, 50, 10, a.gap, f"{run_id}-warmup")
    drain(consumer, 120)

    for rate in [int(r) for r in a.rates.split(",")]:
        step_id = f"{run_id}-r{rate}"
        stats = Stats()
        stats.start()
        res = run_step(producer, consumer, rate, a.duration, a.gap, step_id)
        lag_end = total_lag(consumer)
        drain_s, lag_left = drain(consumer, a.drain_timeout)
        stats.stop.set()
        stats.join()
        time.sleep(a.settle)
        row = {"target_eps": rate, "offered_eps": res["offered_eps"], "sent": res["sent"],
               "sagas": res["sagas"], "lag_end": lag_end, "drain_s": drain_s,
               "lag_after_drain_timeout": lag_left,
               "max_lag": max((l for _, l in res["lag_samples"]), default=0),
               "lag_samples": res["lag_samples"],
               "resources": stats.summary()}
        row.update(db_metrics(step_id, res["t0"], res["t_end"], clock))
        row["lost"] = res["sent"] - row["stored"]
        report["steps"].append(row)
        print(json.dumps({k: v for k, v in row.items() if k != "lag_samples"}, ensure_ascii=False),
              flush=True)

    report["clock_sync"] = clock.summary()
    out = a.out or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "results", f"benchmark_observer_{run_id}.json")
    json.dump(report, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print("Da ghi", out)


if __name__ == "__main__":
    main()
