# 🏥 Telehealth Real-time AI Symptom Checker

A big-data powered telehealth application that uses **Kafka**, **Spark Streaming**, and **Redis** to provide real-time disease predictions based on user symptoms.

---

## 🚀 Getting Started

To run this application, you need to have **Docker**, **Java 17**, and **Python 3.9+** installed.

### 1. Prerequisites & Installation

Clone the repository and install the required Python packages:

```bash
pip install -r requirements.txt
```

### 2. Infrastructure Setup (Docker)

Start the core infrastructure (Zookeeper, Kafka, and Redis) using Docker Compose:

```bash
docker-compose up -d
```
*Wait a few seconds for the services to initialize.*

### 3. Start the AI Processing Engine (Spark)

The Spark Streaming script acts as the "brain," processing symptoms from Kafka and performing AI inference.

```bash
python spark_streaming.py
```
*Ensure you see "✅ Successfully loaded AI model!" in the terminal.*

### 4. Launch the Chatbot (Streamlit)

Finally, start the user interface:

```bash
streamlit run app_chatbot.py
```

---

## 🛠 Features & Monitoring

### 🩺 Automatic Health Checks
When the Streamlit app starts, it automatically pings **Kafka** and **Redis** to ensure the backend is reachable. You will see status logs in the terminal:
- `LOG: [Redis] ✅ Online`
- `LOG: [Kafka] ✅ Online`

### 🔍 Real-time Pipeline Logging
The app provides detailed debug logs in the Streamlit console to track your requests:
1. **📨 Sending:** Logs the unique Request ID sent to Kafka.
2. **🔍 Polling:** Logs when it begins searching Redis for the result.
3. **✅ Success:** Confirms when the result is retrieved from the Spark pipeline.

---

## 🏗 System Architecture

1. **Frontend (Streamlit):** Collects symptoms and sends them to Kafka.
2. **Message Broker (Kafka):** Queues symptom requests for processing.
3. **Processing (Spark Streaming):** Consumes Kafka messages, runs AI models (SBERT + RF), and calculates confidence scores.
4. **Result Store (Redis):** Acts as a low-latency bridge to pass results back to the frontend.

---

## 📊 Evaluation (measured, reproducible)

**Data quality:** `dataset.csv` has 4,920 rows but **93.8% are exact duplicates** → 304 unique symptom profiles across 41 diseases after cleaning (`scripts/data_valuation.py`).

**Model benchmark** (`python scripts/model_training.py`, results in `models/benchmark_report.json`) — all engines use the same Sentence-BERT (`all-MiniLM-L6-v2`) embeddings:

| Engine | Hold-out macro-F1 | 5-fold CV macro-F1 | Median latency / query |
|---|---|---|---|
| SBERT class centroid (cosine) | 1.000 | 1.000 ± 0.000 | 0.19 ms |
| SBERT 1-NN (cosine) — *serving engine* | 0.987 | 0.988 ± 0.011 | 0.28 ms |
| Random Forest on SBERT embeddings | 0.974 | 0.972 ± 0.015 | 13.7 ms |

> ⚠ The dataset contains only templated symptom lists, so near-perfect scores do **not** reflect accuracy on real patient free text.

**End-to-end latency** (`python generalization.py 100` with Kafka, Spark and Redis running): Kafka → Spark Structured Streaming (1 s trigger) → Redis → client poll.
100/100 requests answered, **p50 1.50 s, p95 1.98 s**. Chart: `system_latency_report.png` (raw measurements are written locally to `data/cleaned/latency_measurements.csv`, which is git-ignored).

**Secrets:** `main_worker.py` reads `REDIS_HOST`, `REDIS_PORT` and `REDIS_PASSWORD` from environment variables.
