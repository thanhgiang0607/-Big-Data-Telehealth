import streamlit as st
import json
import time
import uuid
import os
import pandas as pd
import redis
from kafka import KafkaProducer

st.set_page_config(
    page_title="AI Telehealth Big Data Platform", 
    page_icon="🏥", 
    layout="centered"
)

# Premium Theme Contrast Injections
st.markdown("""
    <style>
    .main-title { font-size: 2.2rem; font-weight: 700; color: #0E6655; margin-bottom: 5px; }
    .subtitle-badge { background-color: #E8F8F5; color: #117A65; padding: 4px 12px; border-radius: 12px; font-size: 0.85rem; font-weight: 600; display: inline-block; margin-bottom: 25px; }
    .card-result { background-color: #F4F6F6; border-left: 5px solid #2980B9; padding: 15px; border-radius: 4px 8px 8px 4px; margin-top: 15px; color: #2C3E50 !important; }
    .card-result h4 { color: #1A5276 !important; font-weight: 600; margin-top:0; margin-bottom:10px; }
    .card-result p { color: #34495E !important; margin-bottom:5px; }
    .disease-name { color: #C0392B !important; font-weight: 700; font-size: 1.3rem; margin-top: 10px; display: block; }
    .precaution-box { background-color: #FAFAFA; border: 1px solid #E5E7E9; padding: 15px; border-radius: 8px; margin-top: 15px; color: #2C3E50 !important; }
    .precaution-box h5 { color: #2E4053 !important; font-weight: 600; margin-top:0; margin-bottom:10px; font-size: 1.05rem; }
    .precaution-item { list-style-type: none; margin-bottom: 8px; font-weight: 500; color: #232B2B !important; padding-left: 5px; }
    .precaution-item::before { content: "🎯 "; }
    .disclaimer-text { font-size:0.75rem; color:#7F8C8D; margin-top:15px; display: block; line-height: 1.3; }
    </style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-title">🏥 Real-Time AI Telehealth Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle-badge">🚀 Pipeline: Kafka ➜ Apache Spark ➜ Redis ➜ SBERT Core</div>', unsafe_allow_html=True)

@st.cache_resource
def init_pipeline_connections():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=4000
        )
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        return producer, redis_client
    except Exception:
        return None, None

producer, redis_client = init_pipeline_connections()

@st.cache_data
def load_medical_precautions():
    try:
        df = pd.read_csv("data/cleaned/cleaned_precaution.csv")
        df['Disease_match'] = df['Disease'].astype(str).str.lower().str.strip()
        return df
    except Exception:
        return None

df_precaution = load_medical_precautions()

if "messages" not in st.session_state:
    st.session_state.messages = [{
        "role": "assistant", 
        "content": "Welcome! I am your AI Clinical Assistant operating on an active Big Data distributed pipeline. Please describe your symptoms in detail (e.g., *skin rash, high fever, joint pain*). The streaming framework will analyze the stream and deliver a diagnosis instantly."
    }]

if "patient_id" not in st.session_state:
    st.session_state.patient_id = f"PATIENT-{uuid.uuid4().hex[:6].upper()}"

st.sidebar.image("https://cdn-icons-png.flaticon.com/512/822/822143.png", width=70)
st.sidebar.markdown("### 👤 Session Information")
st.sidebar.markdown(f"**Patient ID:** `{st.session_state.patient_id}`")
st.sidebar.markdown("---")
st.sidebar.markdown("### 🛠️ Infrastructure Status")

if producer and redis_client:
    st.sidebar.success("🟢 Docker Cluster: CONNECTED")
else:
    st.sidebar.error("🔴 Docker Cluster: DISCONNECTED")

# Render History with safe HTML evaluations enabled globally
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"], unsafe_allow_html=True)

if user_input := st.chat_input("Type your symptoms here (e.g., muscle pain, chills)..."):
    with st.chat_message("user"):
        st.markdown(user_input)
    st.session_state.messages.append({"role": "user", "content": user_input})
    
    if not producer or not redis_client:
        st.error("Infrastructure Error: Check Docker containers.")
    else:
        req_id = f"REQ-{uuid.uuid4().hex[:4].upper()}"
        payload = {"request_id": req_id, "patient_id": st.session_state.patient_id, "user_input_symptoms": user_input}
        
        producer.send('telehealth-symptoms', value=payload)
        producer.flush()
        
        with st.chat_message("assistant"):
            with st.spinner("⚡ Spark Streaming executing semantic distributed inference..."):
                redis_key = f"telehealth:result:{st.session_state.patient_id}"
                redis_client.delete(redis_key)
                
                result_found = False
                predicted_disease = ""
                advice_list = []
                
                for attempt in range(35):
                    time.sleep(0.15)
                    cached_data = redis_client.get(redis_key)
                    if cached_data:
                        parsed_res = json.loads(cached_data.decode('utf-8'))
                        if parsed_res.get("request_id") == req_id:
                            predicted_disease = parsed_res["predicted_disease"]
                            if df_precaution is not None:
                                match_rows = df_precaution[df_precaution['Disease_match'] == predicted_disease.lower().strip()]
                                if not match_rows.empty:
                                    precaution_cols = [c for c in df_precaution.columns if 'Precaution' in c]
                                    for col_name in precaution_cols:
                                        advice = match_rows.iloc[0][col_name]
                                        if pd.notnull(advice) and str(advice).strip() not in ["none", ""]:
                                            advice_list.append(str(advice).capitalize())
                            result_found = True
                            break
                
                message_placeholder = st.empty()
                if result_found:
                    base_html = f"""
                    <div class="card-result">
                        <h4>🩺 Real-Time Clinical Analysis Result</h4>
                        <p>Based on distributed stream analytics, the AI model indicates a high probability of:</p>
                        <span class="disease-name">🚨 {predicted_disease}</span>
                    </div>
                    """
                    current_advice_html = ""
                    if advice_list:
                        current_advice_html = '<div class="precaution-box"><h5>📋 Recommended First-Aid & Precautions:</h5>'
                        for item in advice_list:
                            current_advice_html += f'<li class="precaution-item">{item}</li>'
                            full_render = base_html + current_advice_html + "</div>"
                            full_render += '<span class="disclaimer-text">⚠️ <i>Disclaimer: This analysis is an infrastructure simulation of a real-time big data engine and does not constitute certified medical advice.</i></span>'
                            message_placeholder.markdown(full_render, unsafe_allow_html=True)
                            time.sleep(0.20)
                    else:
                        full_render = base_html + '<span class="disclaimer-text">⚠️ <i>Disclaimer: This analysis is an infrastructure simulation of a real-time big data engine and does not constitute certified medical advice.</i></span>'
                        message_placeholder.markdown(full_render, unsafe_allow_html=True)
                    
                    final_html = base_html + (current_advice_html + "</div>" if advice_list else "") + '<span class="disclaimer-text">⚠️ <i>Disclaimer: This analysis is an infrastructure simulation of a real-time big data engine and does not constitute certified medical advice.</i></span>'
                    st.session_state.messages.append({"role": "assistant", "content": final_html})
                else:
                    err = "❌ **Pipeline Timeout:** Distributed computation failure."
                    message_placeholder.markdown(err)
                    st.session_state.messages.append({"role": "assistant", "content": err})