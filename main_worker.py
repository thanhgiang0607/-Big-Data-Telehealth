import os
import json
import time
import pickle
import redis
import numpy as np
import pandas as pd
from fastapi import FastAPI, Request
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

app = FastAPI(title="Telehealth Custom SBERT Calibration Worker")

# ── 1. CLOUD INFRASTRUCTURE CONFIGURATION (UPSTASH REDIS) ──
REDIS_HOST = 'optimum-kit-150562.upstash.io'
REDIS_PORT = 6379
REDIS_PASSWORD = 'gQAAAAAAAkwiAAIgcDE1ZjdjNDI3ZjcwYmI0ZTliYTk4YzJjZTExZDNjMGQyZA'

try:
    redis_db = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD,
        ssl=True, ssl_cert_reqs=None
    )
    print("LOG: [System] Connected to Upstash Redis Cloud successfully!")
except Exception as e:
    print(f"LOG: [System] Redis connection failed: {e}")
    redis_db = None

# ── 2. LOAD UNIFIED SERIALIZED COMPONENTS FROM TRAINING ──
MODEL_DIR = "models/"
COSINE_MIN = 0.25
COSINE_MAX = 0.65

print("LOG: [AI Core] Ingesting serialized calibrated components from training phase...")

# Load core SBERT encoder
sbert_model = SentenceTransformer('all-MiniLM-L6-v2')

# Load Label Encoder to decode class strings
with open(os.path.join(MODEL_DIR, 'label_encoder.pkl'), 'rb') as f:
    label_encoder = pickle.dump = pickle.load(f)

# Load the historical Training Vector Space matrix (X_train & y_train)
with open(os.path.join(MODEL_DIR, 'train_embeddings.pkl'), 'rb') as f:
    train_data = pickle.load(f)
    X_train = train_data['X_train']
    y_train = train_data['y_train']

print(f"LOG: [AI Core] Calibrated Matrix Loaded. Vector Space Size: {X_train.shape}")


@app.get("/")
def check_health():
    return {"status": "healthy", "engine": "Pure SBERT + Min-Max Calibration Layer"}


# ── 3. LIVE INTERSECTION WEBHOOK FROM QSTASH ──
@app.post("/predict")
async def process_prediction(request: Request):
    try:
        raw_body = await request.body()
        payload = json.loads(raw_body.decode('utf-8'))
        
        req_id = payload.get("request_id")
        patient_id = payload.get("patient_id")
        user_input_symptoms = payload.get("user_input_symptoms")
        
        print(f"LOG: [Inference Pipeline] Req ID: {req_id} | Ingested Text: '{user_input_symptoms}'")
        
        if not user_input_symptoms:
            return {"status": "rejected", "reason": "empty_input"}
            
        # Step 1: Compute embedding vector for the patient's dynamic stream query
        test_vector = sbert_model.encode(user_input_symptoms, convert_to_numpy=True)
        
        # Step 2: Compute math pairwise similarities against the entire historical X_train matrix
        raw_similarities = cosine_similarity([test_vector], X_train)[0]
        best_match_idx = np.argmax(raw_similarities)
        raw_score = raw_similarities[best_match_idx]
        
        # Step 3: Decode target vector class to absolute human-readable disease label string
        pred_nlp_code = y_train[best_match_idx]
        predicted_disease = label_encoder.inverse_transform([pred_nlp_code])[0]
        
        # Step 4: Min-Max Calibration Scaling logic execution to calculate confidence percentage
        calibrated_score = (raw_score - COSINE_MIN) / (COSINE_MAX - COSINE_MIN)
        confidence_score = int(calibrated_score * 100)
        confidence_score = max(min(confidence_score, 98), 50)  # Bound mapping matrix limits safely
        
        # Package pipeline output data
        result_payload = {
            "request_id": req_id,
            "patient_id": patient_id,
            "predicted_disease": str(predicted_disease),
            "confidence_score": confidence_score
        }
        
        # Step 5: Broadcast output sync direct to Upstash Redis Cloud
        if redis_db:
            redis_key = f"telehealth:result:{req_id}"
            redis_db.set(redis_key, json.dumps(result_payload), ex=3600)
            print(f"LOG: [Inference Pipeline] ✅ Calibrated matching done. Cached key {redis_key} -> {predicted_disease} ({confidence_score}%)")
            return {"status": "processed", "target_key": redis_key}
            
        return {"status": "internal_cache_error", "reason": "redis_offline"}
        
    except Exception as e:
        print(f"LOG: [Pipeline Exception Error] Failed to compute tensor arrays: {e}")
        return {"status": "runtime_failure", "error": str(e)}