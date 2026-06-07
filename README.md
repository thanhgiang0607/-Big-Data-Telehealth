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
