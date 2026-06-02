import os
import sys
import json
import pickle
import redis
import pandas as pd

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
        
        if not symptoms_text or str(symptoms_text).strip() in ["", "none", "unknown"]:
            predicted_disease = "Unknown Condition"
        else:
            try:
                clean_text = str(symptoms_text).lower().strip()
                test_vector = sbert_model.encode([clean_text])[0]
                pred_code = rf_model.predict([test_vector])[0]
                predicted_disease = str(le.inverse_transform([pred_code])[0]).title()
            except Exception as e:
                predicted_disease = f"Inference Error: {str(e)}"
        
        result_payload = {
            "request_id": row['request_id'],
            "symptoms": symptoms_text,
            "predicted_disease": predicted_disease
        }
        
        r.set(f"telehealth:result:{p_id}", json.dumps(result_payload), ex=3600)
        print(f" 🔥 [Spark Engine] Successfully diagnosed {p_id} -> Result: {predicted_disease}")
        

    log_dir = "data/cleaned/predictions_history"
    os.makedirs(log_dir, exist_ok=True)
    pdf['predicted_disease'] = pdf['user_input_symptoms'].apply(
        lambda x: str(le.inverse_transform([rf_model.predict([sbert_model.encode([str(x).lower().strip()])[0]])[0]])[0]).title()
        if pd.notnull(x) and str(x).strip() not in ["", "none", "unknown"] else "Unknown Condition"
    )
    pdf.to_json(os.path.join(log_dir, f"batch_{batch_id}.json"), orient='records', lines=True)

query = df_parsed.writeStream \
    .foreachBatch(foreach_batch_sink) \
    .trigger(processingTime='1 second') \
    .start()

print("🚀 Spark Streaming Engine is running and listening to Kafka topic...")
query.awaitTermination()