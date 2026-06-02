import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

print("📡 KAFKA PRODUCER: SIMULATING REAL-TIME PATIENT STREAM")
print("-" * 60)

#Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✔️ Successfully connected to Kafka Broker on port 9092.")
except Exception as e:
    print(f"❌ Kafka connection error: {e}")
    exit()

#Read clean symptom file
try:
    df = pd.read_csv('data/cleaned/cleaned_dataset.csv')
    symptoms_list = df['Cleaned_Symptoms_Text'].dropna().tolist()
except Exception as e:
    print(f"⚠️ Cannot read clean file, using default sample. Details: {e}")
    symptoms_list = ["headache, vomiting, chills", "skin rash, high fever, itching"]

counter = 1
print("\n🔥 Start sending data (Press Ctrl + C to stop)...")
try:
    while True:
        random_symptom = random.choice(symptoms_list)
        req_id = f"REQ-{100 + counter}"
        p_id = f"PATIENT-{random.randint(1000, 9999)}"
        
        payload = {
            "request_id": req_id,
            "patient_id": p_id,
            "user_input_symptoms": str(random_symptom)
        }
        
        # Send data to Kafka topic
        producer.send('telehealth-symptoms', value=payload)
        print(f"📥 [Sent #{counter}] -> {req_id} | {p_id} | Symptoms: \"{random_symptom[:50]}...\"")
        
        counter += 1
        time.sleep(2) # Every 2 seconds, a new patient will appear

except KeyboardInterrupt:
    print("\n🛑 Manually stopped simulated data stream.")
finally:
    producer.flush()