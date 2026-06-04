import os
import sys
import json
import pickle
import redis
import pandas as pd
import re
import numpy as np  
from sklearn.metrics.pairwise import cosine_similarity 

os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField

spark = SparkSession.builder \
    .appName("Telehealth-Realtime-Inference") \
    .master("local[*]") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🧠 Loading AI brain into the main system...")
MODEL_DIR = "models/"
with open(os.path.join(MODEL_DIR, 'sbert_model.pkl'), 'rb') as f: sbert_model = pickle.load(f)
with open(os.path.join(MODEL_DIR, 'label_encoder.pkl'), 'rb') as f: le = pickle.load(f)
with open(os.path.join(MODEL_DIR, 'rf_model.pkl'), 'rb') as f: rf_model = pickle.load(f)
with open(os.path.join(MODEL_DIR, 'train_embeddings.pkl'), 'rb') as f: train_data = pickle.load(f)
print("✅ Successfully loaded AI model!")

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "telehealth-symptoms") \
    .option("startingOffsets", "latest") \
    .load()

df_string = df_kafka.selectExpr("CAST(value AS STRING) as json_payload")

schema = StructType([
    StructField("request_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("user_input_symptoms", StringType(), True)
])
df_parsed = df_string.withColumn("data", from_json(col("json_payload"), schema)).select("data.*")


def foreach_batch_sink(df_batch, batch_id):
    if df_batch.count() == 0:
        return
    
    pdf = df_batch.toPandas()
    r = redis.Redis(host='localhost', port=6379, db=0)
    
    for idx, row in pdf.iterrows():
        p_id = row['patient_id']
        symptoms_text = row['user_input_symptoms']
        confidence_score = 0 
        
        if not symptoms_text or str(symptoms_text).strip() in ["", "none", "unknown"]:
            predicted_disease = "Unknown Condition"
            confidence_score = 0
        else:
            try:
                # Đồng bộ hoàn chỉnh xử lý chuỗi văn bản lâm sàng
                clean_text = str(symptoms_text).lower().strip()
                clean_text = clean_text.replace('_', ' ')
                
                vector = sbert_model.encode([clean_text])
                
                X_train = train_data['X_train']
                y_train = train_data['y_train']
                
                raw_similarities = cosine_similarity(vector, X_train)[0]
                best_match_idx = np.argmax(raw_similarities)
                best_raw_cosine = raw_similarities[best_match_idx]
                
                predicted_label_code = y_train[best_match_idx]
                predicted_disease = str(le.inverse_transform([predicted_label_code])[0]).title()
                
                COSINE_MIN, COSINE_MAX = 0.25, 0.65
                clipped_cosine = max(COSINE_MIN, min(COSINE_MAX, best_raw_cosine))
                confidence_score = int(((clipped_cosine - COSINE_MIN) / (COSINE_MAX - COSINE_MIN)) * 100)
                
                print(f"🎯 [Spark Serving Layer] Diagnosed: {predicted_disease} ({confidence_score}%)")
                
            except Exception as e:
                predicted_disease = f"Inference Error"
                confidence_score = 50
                print(f"❌ [Spark Engine Error]: {str(e)}")
        
        result_payload = {
            "request_id": row['request_id'],
            "symptoms": symptoms_text,
            "predicted_disease": predicted_disease,
            "confidence": confidence_score
        }
        
        r.set(f"telehealth:result:{p_id}", json.dumps(result_payload), ex=3600)
        print(f" 🔥 [Spark Engine] Diagnosed {p_id} -> {predicted_disease} ({confidence_score}%)")
        
    log_dir = "data/cleaned/predictions_history"
    os.makedirs(log_dir, exist_ok=True)
    pdf.to_json(os.path.join(log_dir, f"batch_{batch_id}.json"), orient='records', lines=True)


query = df_parsed.writeStream \
    .foreachBatch(foreach_batch_sink) \
    .trigger(processingTime='1 second') \
    .start()

print("🚀 Spark Streaming Engine is running and listening to Kafka topic...")
query.awaitTermination()