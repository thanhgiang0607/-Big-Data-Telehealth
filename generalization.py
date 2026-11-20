"""
Measure REAL end-to-end latency of the streaming pipeline:
  Kafka produce  ->  Spark Structured Streaming inference  ->  Redis result  ->  client poll

Prerequisites (see README): `docker-compose up -d` and `python spark_streaming.py` running.
Usage: python generalization.py [num_requests]
Outputs: system_latency_report.png and data/cleaned/latency_measurements.csv
"""
import os
import sys
import json
import time
import uuid
import random
import numpy as np
import pandas as pd
import redis
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from kafka import KafkaProducer

N_REQUESTS = int(sys.argv[1]) if len(sys.argv) > 1 else 100
TOPIC = "telehealth-symptoms"
TIMEOUT_S = 30
GAP_S = 0.5  # pause between requests so each one is measured in isolation

producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))
r = redis.Redis(host="localhost", port=6379, db=0)

try:
    symptoms = pd.read_csv("data/cleaned/cleaned_dataset.csv")["Cleaned_Symptoms_Text"].dropna().tolist()
except FileNotFoundError:
    symptoms = ["headache, vomiting, chills", "skin rash, high fever, itching"]

random.seed(42)
rows = []
print(f"Sending {N_REQUESTS} requests and measuring end-to-end latency...")
for i in range(1, N_REQUESTS + 1):
    patient_id = f"BENCH-{uuid.uuid4().hex[:10]}"
    key = f"telehealth:result:{patient_id}"
    payload = {"request_id": f"REQ-{uuid.uuid4().hex[:8]}", "patient_id": patient_id,
               "user_input_symptoms": random.choice(symptoms)}

    t0 = time.perf_counter()
    producer.send(TOPIC, value=payload)
    producer.flush()
    latency = None
    while time.perf_counter() - t0 < TIMEOUT_S:
        if r.exists(key):
            latency = time.perf_counter() - t0
            break
        time.sleep(0.01)
    rows.append({"request": i, "latency_s": latency})
    print(f"  #{i:3d}  {'TIMEOUT' if latency is None else f'{latency:.3f}s'}")
    r.delete(key)
    time.sleep(GAP_S)

df = pd.DataFrame(rows)
os.makedirs("data/cleaned", exist_ok=True)
df.to_csv("data/cleaned/latency_measurements.csv", index=False)

ok = df["latency_s"].dropna()
if ok.empty:
    raise SystemExit("No responses received - is spark_streaming.py running?")
print(f"\nCompleted {len(ok)}/{N_REQUESTS} | p50 {ok.median():.3f}s | p95 {ok.quantile(0.95):.3f}s | max {ok.max():.3f}s")

plt.figure(figsize=(11, 5.5))
plt.plot(df["request"], df["latency_s"], color="#00C2A8", linewidth=2, label="Measured end-to-end latency")
plt.axhline(ok.median(), color="#1f77b4", linestyle="--", label=f"p50 = {ok.median():.2f}s")
plt.axhline(ok.quantile(0.95), color="#FF6B6B", linestyle=":", label=f"p95 = {ok.quantile(0.95):.2f}s")
plt.title("Kafka -> Spark Streaming -> Redis: measured end-to-end latency", fontweight="bold")
plt.xlabel("Request #")
plt.ylabel("Latency (s)")
plt.legend(loc="upper right")
plt.tight_layout()
plt.savefig("system_latency_report.png", dpi=200)
print("Saved system_latency_report.png and data/cleaned/latency_measurements.csv")
