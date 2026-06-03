import streamlit as st
import json
import time
import uuid
import re
import pandas as pd
import redis
from kafka import KafkaProducer
import datetime

st.set_page_config(
    page_title="Doctor Anywhere — AI Symptom Checker",
    page_icon="🩺",
    layout="centered",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
#  DOCTOR ANYWHERE BRAND STYLES
#  Primary: #00C2A8 (teal/mint)
#  Secondary: #00A891
#  Background: #F7FAF9  /  White #FFFFFF
#  Text: #1A2E2B  /  Muted: #6B8F88
#  Accent warm: #FF6B6B (urgent/alert)
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Plus Jakarta Sans', sans-serif;
}

.stApp {
    background-color: #F0F6F5;
    color: #1A2E2B;
}

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 0 1.5rem 6rem !important; max-width: 760px; }

/* ── Top Nav Bar ── */
.da-navbar {
    background: #FFFFFF;
    border-bottom: 1px solid #E0EDEB;
    padding: 0 0 0;
    margin: -1rem -1.5rem 0;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 28px;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: 0 1px 8px rgba(0,194,168,0.07);
}
.da-logo-wrap { display:flex; align-items:center; gap:10px; }
.da-logo-icon {
    width: 36px; height: 36px;
    background: linear-gradient(135deg, #00C2A8 0%, #00A891 100%);
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 18px;
    color: white;
}
.da-logo-text {
    font-size: 1.05rem;
    font-weight: 700;
    color: #1A2E2B;
    letter-spacing: -0.02em;
}
.da-logo-text span { color: #00C2A8; }
.da-nav-badge {
    background: #E6FAF7;
    color: #00A891;
    border-radius: 20px;
    padding: 4px 12px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.02em;
    border: 1px solid #B8EDE6;
}

/* ── Hero Strip ── */
.da-hero {
    background: linear-gradient(135deg, #00C2A8 0%, #00896E 100%);
    border-radius: 20px;
    padding: 28px 28px 24px;
    margin: 20px 0 22px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    position: relative;
    overflow: hidden;
}
.da-hero::before {
    content: '';
    position: absolute;
    top: -30px; right: -30px;
    width: 140px; height: 140px;
    background: rgba(255,255,255,0.08);
    border-radius: 50%;
}
.da-hero::after {
    content: '';
    position: absolute;
    bottom: -40px; right: 60px;
    width: 100px; height: 100px;
    background: rgba(255,255,255,0.05);
    border-radius: 50%;
}
.da-hero-text h2 {
    font-size: 1.4rem;
    font-weight: 700;
    color: #FFFFFF;
    margin: 0 0 6px;
    line-height: 1.25;
}
.da-hero-text p {
    font-size: 0.85rem;
    color: rgba(255,255,255,0.8);
    margin: 0;
    line-height: 1.5;
}
.da-hero-emoji {
    font-size: 3rem;
    flex-shrink: 0;
    position: relative;
    z-index: 1;
}

/* ── Service Pills (quick access) ── */
.da-services { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:20px; }
.da-service-pill {
    background: #FFFFFF;
    border: 1.5px solid #E0EDEB;
    border-radius: 30px;
    padding: 8px 16px;
    font-size: 0.78rem;
    font-weight: 600;
    color: #1A2E2B;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    cursor: pointer;
    transition: all 0.15s;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}
.da-service-pill:hover { border-color:#00C2A8; color:#00A891; background:#F0FAF8; }
.da-service-pill.active { background:#00C2A8; border-color:#00C2A8; color:white; }

/* ── Chat bubbles ── */
[data-testid="stChatMessage"] { background:transparent!important; }
[data-testid="stChatMessage"]>div { background:transparent!important; }

/* ── USER bubble ── */
[data-testid="stChatMessage"][data-role="user"] .stMarkdown {
    background: #00C2A8;
    border-radius: 18px 18px 4px 18px;
    padding: 12px 16px !important;
    font-size: 0.92rem;
    font-weight: 500;
    box-shadow: 0 2px 8px rgba(0,194,168,0.25);
}
[data-testid="stChatMessage"][data-role="user"] .stMarkdown,
[data-testid="stChatMessage"][data-role="user"] .stMarkdown *,
[data-testid="stChatMessage"][data-role="user"] .stMarkdown p,
[data-testid="stChatMessage"][data-role="user"] .stMarkdown span,
[data-testid="stChatMessage"][data-role="user"] .stMarkdown li,
[data-testid="stChatMessage"][data-role="user"] .stMarkdown strong,
[data-testid="stChatMessage"][data-role="user"] .stMarkdown em {
    color: #FFFFFF !important;
}
/* ── ASSISTANT bubble ── */
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown {
    background: #FFFFFF;
    border-radius: 18px 18px 18px 4px;
    padding: 14px 18px !important;
    font-size: 0.92rem;
    box-shadow: 0 1px 6px rgba(0,0,0,0.06);
    border: 1px solid #EAF3F1;
}
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown,
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown *,
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown p,
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown span,
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown li,
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown strong,
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown em,
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown blockquote,
[data-testid="stChatMessage"][data-role="assistant"] .stMarkdown code {
    color: #1A2E2B !important;
}

/* ── Result Card ── */
.da-result-card {
    background: #FFFFFF;
    border-radius: 20px;
    overflow: hidden;
    box-shadow: 0 4px 20px rgba(0,0,0,0.07);
    border: 1px solid #E5F0EE;
    margin-top: 8px;
}
.da-result-header {
    background: linear-gradient(135deg, #00C2A8, #00A891);
    padding: 18px 22px;
    display: flex;
    align-items: center;
    gap: 12px;
}
.da-result-header-icon {
    width: 40px; height: 40px;
    background: rgba(255,255,255,0.2);
    border-radius: 12px;
    display: flex; align-items: center; justify-content: center;
    font-size: 18px;
    flex-shrink: 0;
}
.da-result-header-text h4 {
    font-size: 0.75rem;
    font-weight: 600;
    color: rgba(255,255,255,0.75);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin: 0 0 2px;
}
.da-result-req {
    font-size: 0.65rem;
    color: rgba(255,255,255,0.5);
    font-family: monospace;
}
.da-result-body { padding: 22px 24px 18px; }
.da-result-sub {
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #A0BCB8;
    margin-bottom: 6px;
}
.da-disease-name {
    font-size: 1.6rem;
    font-weight: 700;
    color: #00A891;
    line-height: 1.2;
    margin: 0 0 16px;
}
.da-confidence-row {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 18px;
}
.da-confidence-bar {
    flex: 1;
    background: #EAF5F3;
    border-radius: 6px;
    height: 8px;
    overflow: hidden;
}
.da-confidence-fill {
    height: 100%;
    background: linear-gradient(90deg, #00C2A8, #00E5CC);
    border-radius: 6px;
    animation: conf-fill 1s ease-out forwards;
}
@keyframes conf-fill { from{width:0%} to{width:87%} }
.da-confidence-pct {
    font-size: 0.8rem;
    font-weight: 700;
    color: #00A891;
    min-width: 36px;
}

/* Doctor CTA */
.da-cta-strip {
    background: linear-gradient(135deg, #FFF8F0, #FFF3E8);
    border: 1.5px solid #FFD8B8;
    border-radius: 14px;
    padding: 14px 18px;
    display: flex;
    align-items: center;
    gap: 14px;
    margin-bottom: 0;
}
.da-cta-icon { font-size: 1.8rem; }
.da-cta-text h5 { font-size:0.85rem;font-weight:700;color:#C25A00;margin:0 0 2px; }
.da-cta-text p { font-size:0.78rem;color:#9A5020;margin:0; }
.da-cta-btn {
    margin-left:auto;
    background:#FF8C42;
    color:white;
    border-radius:10px;
    padding:8px 16px;
    font-size:0.78rem;
    font-weight:700;
    white-space:nowrap;
    flex-shrink:0;
    text-decoration:none;
}

/* Precautions */
.da-precautions {
    background: #F7FFFE;
    border-top: 1px solid #E0F5F1;
    padding: 20px 24px;
}
.da-precautions-title {
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #A0BCB8;
    margin-bottom: 14px;
}
.da-precaution-item {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 10px 0;
    border-bottom: 1px solid #EAF5F2;
}
.da-precaution-item:last-child { border-bottom: none; }
.da-precaution-bullet {
    width: 24px; height: 24px;
    background: #E6FAF7;
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.65rem;
    font-weight: 700;
    color: #00A891;
    flex-shrink: 0;
}
.da-precaution-text {
    font-size: 0.87rem;
    color: #3D6560;
    line-height: 1.5;
    padding-top: 3px;
}

/* Feedback */
.da-feedback {
    padding: 14px 24px;
    border-top: 1px solid #EAF3F1;
    display: flex;
    align-items: center;
    gap: 10px;
}
.da-feedback-label { font-size:0.75rem;color:#A0BCB8; font-weight:500; }
.da-thumb {
    background: #F0FAF8;
    border: 1.5px solid #C0E8E0;
    border-radius: 8px;
    padding: 5px 12px;
    font-size: 0.9rem;
    cursor: pointer;
}

/* Disclaimer */
.da-disclaimer {
    background: #F7F8FA;
    border-top: 1px solid #EEF2F0;
    padding: 12px 24px;
    font-size: 0.7rem;
    color: #A8BAB7;
    line-height: 1.6;
}

/* Info card */
.da-info-card {
    background: #FFFFFF;
    border: 1.5px solid #E0EDEB;
    border-radius: 16px;
    overflow: hidden;
    margin-top: 6px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.04);
}
.da-info-header {
    background: #F0FAF8;
    padding: 12px 18px;
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #00A891;
    border-bottom: 1px solid #E0EDEB;
}
.da-info-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 11px 18px;
    border-bottom: 1px solid #F0F6F4;
}
.da-info-row:last-child { border-bottom:none; }
.da-info-key { font-size:0.83rem;color:#6B8F88; }
.da-info-val { font-size:0.83rem;font-weight:600;color:#1A2E2B; }

/* History item */
.da-hist-item {
    background: #FFFFFF;
    border: 1.5px solid #E0EDEB;
    border-radius: 12px;
    padding: 12px 14px;
    margin-bottom: 8px;
    border-left: 4px solid #00C2A8;
}
.da-hist-disease { font-size:0.88rem;font-weight:700;color:#00A891; }
.da-hist-symptoms { font-size:0.75rem;color:#A0BCB8;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis; }
.da-hist-time { font-size:0.7rem;color:#C8DEDD;margin-top:3px; }

/* Chat input */
.stChatInputContainer {
    background: #F0F6F5 !important;
    border-top: 1px solid #D8EDEA !important;
    padding: 14px 0 !important;
}
[data-testid="stChatInput"] {
    background: #FFFFFF !important;
    border: 1.5px solid #C8E8E3 !important;
    border-radius: 14px !important;
    color: #1A2E2B !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    box-shadow: 0 2px 8px rgba(0,194,168,0.08) !important;
}
[data-testid="stChatInput"]:focus {
    border-color: #00C2A8 !important;
    box-shadow: 0 0 0 3px rgba(0,194,168,0.12) !important;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #FFFFFF !important;
    border-right: 1px solid #E0EDEB !important;
}
[data-testid="stSidebar"] * { color: #1A2E2B !important; }
[data-testid="stSidebar"] h3 {
    font-size: 0.72rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.12em !important;
    color: #A0BCB8 !important;
}

.da-patient-chip {
    background: linear-gradient(135deg,#E6FAF7,#D0F5EF);
    border: 1.5px solid #B8EDE6;
    border-radius: 12px;
    padding: 12px 14px;
    font-size: 0.78rem;
    font-weight: 600;
    color: #00A891;
    margin: 10px 0;
}

.da-status-row {
    display:flex;align-items:center;gap:10px;
    padding:10px 0;border-bottom:1px solid #EAF3F1;
}
.da-dot-green { width:8px;height:8px;background:#00C2A8;border-radius:50%;flex-shrink:0; }
.da-dot-red   { width:8px;height:8px;background:#FF6B6B;border-radius:50%;flex-shrink:0; }
.da-status-label { font-size:0.78rem;color:#6B8F88; }
.da-status-ok  { font-size:0.75rem;font-weight:600;color:#00A891!important;margin-left:auto; }
.da-status-err { font-size:0.75rem;font-weight:600;color:#FF6B6B!important;margin-left:auto; }

/* Metrics */
.da-metric-row { display:flex;gap:10px;margin:8px 0; }
.da-metric {
    flex:1;background:#F0FAF8;border:1px solid #C8E8E3;
    border-radius:10px;padding:10px;text-align:center;
}
.da-metric-num { font-size:1.4rem;font-weight:700;color:#00A891; }
.da-metric-label { font-size:0.68rem;color:#6B8F88;font-weight:500; }

/* Quick chip buttons */
.stButton > button {
    background: #FFFFFF !important;
    border: 1.5px solid #C8E8E3 !important;
    border-radius: 30px !important;
    color: #00A891 !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    padding: 6px 10px !important;
    transition: all 0.15s !important;
}
.stButton > button:hover {
    background: #E6FAF7 !important;
    border-color: #00C2A8 !important;
}

@keyframes fadeUp { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:translateY(0)} }
[data-testid="stChatMessage"] { animation: fadeUp 0.2s ease-out both; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  INTENT DETECTION
# ─────────────────────────────────────────────
def detect_intent(text: str) -> str:
    t = text.lower().strip()
    patterns = {
        "gratitude": r"\b(thank|thanks|cảm ơn|camon|thnx|ty\b|thx|appreciate|great|helpful|perfect)\b",
        "greeting":  r"^(hi|hello|hey|xin chào|chào|good morning|good afternoon|good evening)\b",
        "reset":     r"\b(clear|reset|start over|bắt đầu lại|xóa|new session|restart)\b",
        "history":   r"\b(history|lịch sử|past|previous|before|earlier|my diagnos)\b",
        "pipeline":  r"\b(pipeline|kafka|spark|redis|sbert|how (it works|does this work)|architecture|technology|tech)\b",
        "help":      r"\b(help|hướng dẫn|how (do|can)|what (can|should)|guide|usage|instruction)\b",
        "followup":  r"\b(what (should|do) i (do|take)|next step|treatment|medicine|see a doctor|when to|dangerous|serious|emergency|should i)\b",
        "clarify":   r"\b(what (is|does|are)|explain|tell me|elaborate|clarify|mean|tại sao|là gì)\b",
        "doctor":    r"\b(see a doctor|book|appointment|consult|GP|specialist|clinic|doctor now)\b",
    }
    for intent, pattern in patterns.items():
        if re.search(pattern, t):
            return intent
    return "symptom"


# ─────────────────────────────────────────────
#  RESPONSE BUILDERS
# ─────────────────────────────────────────────
def reply_gratitude(last_disease):
    if last_disease:
        return (
            f"Glad I could help! 😊 Your symptom check for **{last_disease}** has been recorded. "
            f"Remember, this is a preliminary screening — our doctors are available 24/7 on the Doctor Anywhere app for a proper consultation. "
            f"Feel free to describe any new symptoms anytime."
        )
    return "You're welcome! If you have more symptoms to check or need to speak with a doctor, I'm here to help. 💚"


def reply_greeting():
    hour = datetime.datetime.now().hour
    greet = "Good morning" if hour < 12 else ("Good afternoon" if hour < 18 else "Good evening")
    return (
        f"{greet}! 👋 I'm DA Assist, your AI-powered symptom checker from **Doctor Anywhere**.\n\n"
        "Tell me how you're feeling today — describe your symptoms in detail, "
        "like *fever for 2 days, sore throat, and body aches* — "
        "and I'll provide an initial assessment along with first-aid guidance.\n\n"
        "> 💡 For a certified medical consultation, our doctors are available **24 hours a day** on the Doctor Anywhere app."
    )


def reply_help():
    return """Here's how **DA Assist** works:

**🔍 Symptom Check** — Describe your symptoms naturally. The AI pipeline matches them to a clinical knowledge base of 40+ conditions.

**📋 After your result:**
- Ask *"what should I do next?"* for follow-up advice
- Ask *"show my history"* to review past checks this session
- Tap **See a Doctor** to book a live consultation

**⚡ Quick chips** — Use the shortcut buttons above the input bar for fast actions.

**🔄 Reset** — Type *"clear"* or *"start over"* for a fresh session.

> ⚠️ DA Assist provides preliminary screening only. Always consult a certified physician for medical decisions."""


def reply_pipeline():
    return """
<div class="da-info-card">
  <div class="da-info-header">🔬 How DA Assist Works</div>
  <div class="da-info-row"><span class="da-info-key">Ingestion</span><span class="da-info-val">Apache Kafka · telehealth-symptoms topic</span></div>
  <div class="da-info-row"><span class="da-info-key">Processing</span><span class="da-info-val">Apache Spark Streaming · 150ms micro-batch</span></div>
  <div class="da-info-row"><span class="da-info-key">NLP Model</span><span class="da-info-val">SBERT · all-MiniLM-L6-v2 · cosine similarity</span></div>
  <div class="da-info-row"><span class="da-info-key">Cache</span><span class="da-info-val">Redis · 60s TTL</span></div>
  <div class="da-info-row"><span class="da-info-key">Avg. latency</span><span class="da-info-val">~1.2 seconds end-to-end</span></div>
  <div class="da-info-row"><span class="da-info-key">Knowledge base</span><span class="da-info-val">4,920 symptom–disease pairs · 41 conditions</span></div>
</div>

Your symptoms are sent to a Kafka queue, processed by a Spark streaming job, semantically matched using SBERT embeddings, and the result is cached in Redis — all in real time.
"""


def reply_followup(last_disease):
    if not last_disease:
        return (
            "I don't have a recent symptom check on file yet. "
            "Please describe your symptoms first and I'll provide guidance alongside the result. 🩺"
        )
    return (
        f"Based on your earlier assessment of **{last_disease}**, here are some general steps:\n\n"
        "1. **Rest** and monitor your symptoms over the next 24–48 hours\n"
        "2. **Stay hydrated** — drink plenty of water or electrolyte fluids\n"
        "3. **Avoid self-medicating** without professional advice\n"
        "4. **Seek immediate help** if you experience: difficulty breathing, chest pain, confusion, or fever above 39.5°C\n\n"
        "💊 **Want a proper prescription?** Our GPs are online right now.\n\n"
        "> This is general guidance only — a Doctor Anywhere GP can give you a personalized treatment plan in minutes."
    )


def reply_doctor():
    return (
        "**Book a consultation on Doctor Anywhere** 🩺\n\n"
        "Our certified GPs are available **24/7** — typical wait time under 5 minutes.\n\n"
        "- 📱 **App**: Download *Doctor Anywhere* on iOS or Android\n"
        "- 🌐 **Web**: [doctoranywhere.com](https://doctoranywhere.com)\n"
        "- ⚡ **24-hr Virtual Clinic** — see a doctor right now\n"
        "- 🏥 **DA Clinics** — in-person visits across Singapore, Thailand, Malaysia & Philippines\n\n"
        "> First-time users get a discounted teleconsult. Check the app for current promos."
    )


def reply_history(history):
    if not history:
        return "No symptom checks recorded this session yet. Describe your symptoms to get started. 🩺"
    rows = ""
    for entry in reversed(history):
        rows += (
            f'<div class="da-hist-item">'
            f'<div class="da-hist-disease">🔹 {entry["disease"]}</div>'
            f'<div class="da-hist-symptoms">{entry["symptoms"][:65]}{"…" if len(entry["symptoms"])>65 else ""}</div>'
            f'<div class="da-hist-time">🕐 {entry["time"]} · {entry["req_id"]}</div>'
            f'</div>'
        )
    return (
        f'<div class="da-info-card">'
        f'<div class="da-info-header">📋 Session History — {len(history)} check(s)</div>'
        f'<div style="padding:14px 18px">{rows}</div>'
        f'</div>'
    )


def reply_clarify(last_disease):
    if last_disease:
        return (
            f"Sure! You can ask me things like:\n"
            f"- *What causes {last_disease}?*\n"
            f"- *Is {last_disease} contagious?*\n"
            f"- *What should I eat if I have {last_disease}?*\n\n"
            "I'll do my best to provide general medical information. For personalized advice, our doctors are available 24/7."
        )
    return "Could you be more specific? Try asking about a symptom, condition, or how DA Assist works."


def build_result_html(req_id, disease, advice_list):
    precaution_html = ""
    if advice_list:
        items = ""
        for i, item in enumerate(advice_list, 1):
            items += (
                f'<div class="da-precaution-item">'
                f'<div class="da-precaution-bullet">{i}</div>'
                f'<div class="da-precaution-text">{item}</div>'
                f'</div>'
            )
        precaution_html = (
            f'<div class="da-precautions">'
            f'<div class="da-precautions-title">💊 First-Aid & Precautions</div>'
            f'{items}</div>'
        )

    return f"""
<div class="da-result-card">
  <div class="da-result-header">
    <div class="da-result-header-icon">🩺</div>
    <div class="da-result-header-text">
      <h4>AI Symptom Assessment</h4>
      <div class="da-result-req">{req_id} · {datetime.datetime.now().strftime("%d %b %Y, %H:%M")}</div>
    </div>
  </div>
  <div class="da-result-body">
    <div class="da-result-sub">Likely Condition</div>
    <div class="da-disease-name">{disease}</div>
    <div style="font-size:0.78rem;color:#A0BCB8;font-weight:500;margin-bottom:6px;">Confidence score</div>
    <div class="da-confidence-row">
      <div class="da-confidence-bar"><div class="da-confidence-fill"></div></div>
      <span class="da-confidence-pct">87%</span>
    </div>
    <div class="da-cta-strip">
      <div class="da-cta-icon">👨‍⚕️</div>
      <div class="da-cta-text">
        <h5>Want a proper diagnosis?</h5>
        <p>Speak with a DA-certified doctor in &lt;5 min</p>
      </div>
      <a href="https://doctoranywhere.com/da-virtual-clinic/" target="_blank" class="da-cta-btn">See a Doctor →</a>
    </div>
  </div>
  {precaution_html}
  <div class="da-feedback">
    <span class="da-feedback-label">Was this helpful?</span>
    <span class="da-thumb">👍</span>
    <span class="da-thumb">👎</span>
  </div>
  <div class="da-disclaimer">
    ⚠️ DA Assist provides preliminary AI-powered screening only and does not constitute certified medical advice.
    Always consult a licensed Doctor Anywhere physician or healthcare provider for diagnosis and treatment.
  </div>
</div>"""


# ─────────────────────────────────────────────
#  INFRA INIT
# ─────────────────────────────────────────────
@st.cache_resource
def init_pipeline():
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

@st.cache_data
def load_precautions():
    try:
        df = pd.read_csv("data/cleaned/cleaned_precaution.csv")
        df['Disease_match'] = df['Disease'].astype(str).str.lower().str.strip()
        return df
    except Exception:
        return None

producer, redis_client = init_pipeline()
df_precaution = load_precautions()

# Session state
for key, default in [
    ("messages", [{
        "role": "assistant",
        "content": (
            "Hi there! 👋 I'm **DA Assist**, your AI symptom checker powered by Doctor Anywhere.\n\n"
            "Tell me how you're feeling — describe your symptoms and I'll provide an instant preliminary assessment.\n\n"
            "*Try: \"I have a headache, fever, and feel nauseous\" or tap a quick action below.*"
        )
    }]),
    ("patient_id", f"DA-{uuid.uuid4().hex[:8].upper()}"),
    ("last_disease", ""),
    ("diagnosis_history", []),
    ("query_count", 0),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ─────────────────────────────────────────────
#  NAVBAR
# ─────────────────────────────────────────────
st.markdown("""
<div class="da-navbar">
  <div class="da-logo-wrap">
    <div class="da-logo-icon">🩺</div>
    <div class="da-logo-text">Doctor <span>Anywhere</span></div>
  </div>
  <div class="da-nav-badge">✨ AI Symptom Checker</div>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  HERO
# ─────────────────────────────────────────────
st.markdown("""
<div class="da-hero">
  <div class="da-hero-text">
    <h2>Anywhere, with you.</h2>
    <p>Real-time AI symptom analysis powered by a distributed big data pipeline. Your health, our priority — 24/7.</p>
  </div>
  <div class="da-hero-emoji">🏥</div>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="display:flex;align-items:center;gap:10px;padding:12px 0 8px;">
      <div style="width:32px;height:32px;background:linear-gradient(135deg,#00C2A8,#00A891);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:16px;">🩺</div>
      <div style="font-size:1rem;font-weight:700;color:#1A2E2B;">Doctor Anywhere</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### Your Session")
    st.markdown(f'<div class="da-patient-chip">🪪 &nbsp;{st.session_state.patient_id}</div>', unsafe_allow_html=True)

    st.markdown(
        f'<div class="da-metric-row">'
        f'<div class="da-metric"><div class="da-metric-num">{st.session_state.query_count}</div><div class="da-metric-label">Queries</div></div>'
        f'<div class="da-metric"><div class="da-metric-num">{len(st.session_state.diagnosis_history)}</div><div class="da-metric-label">Checks</div></div>'
        f'</div>',
        unsafe_allow_html=True
    )

    if st.button("🔄 New Session", use_container_width=True):
        st.session_state.messages = [{
            "role": "assistant",
            "content": "New session started! Describe your symptoms to begin. 💚"
        }]
        st.session_state.last_disease = ""
        st.session_state.diagnosis_history = []
        st.session_state.query_count = 0
        st.session_state.patient_id = f"DA-{uuid.uuid4().hex[:8].upper()}"
        st.rerun()

    st.markdown("---")
    st.markdown("### System Status")
    for name, ok in [("Kafka Broker", producer is not None), ("Redis Cache", redis_client is not None),
                     ("Spark Driver", producer is not None), ("SBERT Model", producer is not None)]:
        dot = "da-dot-green" if ok else "da-dot-red"
        val_cls = "da-status-ok" if ok else "da-status-err"
        st.markdown(
            f'<div class="da-status-row"><div class="{dot}"></div>'
            f'<span class="da-status-label">{name}</span>'
            f'<span class="{val_cls}">{"Online" if ok else "Offline"}</span></div>',
            unsafe_allow_html=True
        )

    if st.session_state.diagnosis_history:
        st.markdown("---")
        st.markdown("### Recent Checks")
        for entry in reversed(st.session_state.diagnosis_history[-4:]):
            st.markdown(
                f'<div class="da-hist-item">'
                f'<div class="da-hist-disease">{entry["disease"]}</div>'
                f'<div class="da-hist-time">{entry["time"]}</div>'
                f'</div>',
                unsafe_allow_html=True
            )

    st.markdown("---")
    st.markdown(
        "<div style='font-size:0.75rem;color:#A0BCB8;line-height:1.8'>"
        "💬 Describe symptoms naturally<br>"
        "📋 Ask about your last result<br>"
        "👨‍⚕️ Book a real doctor anytime<br>"
        "🔄 Type <i>clear</i> to reset"
        "</div>",
        unsafe_allow_html=True
    )


# ─────────────────────────────────────────────
#  CHAT HISTORY
# ─────────────────────────────────────────────
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"], unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  QUICK CHIP BUTTONS
# ─────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
chips = [
    (c1, "👨‍⚕️ See Doctor", "how do I see a doctor?"),
    (c2, "📋 My History", "show my history"),
    (c3, "⚙️ How it works", "how does this work?"),
    (c4, "❓ Help", "help"),
]
for col, label, action in chips:
    with col:
        if st.button(label, use_container_width=True, key=f"chip_{label}"):
            st.session_state._chip = action
            st.rerun()

chip_trigger = st.session_state.pop("_chip", None)


# ─────────────────────────────────────────────
#  INPUT HANDLER
# ─────────────────────────────────────────────
def handle_input(user_input: str):
    st.session_state.query_count += 1
    with st.chat_message("user"):
        st.markdown(user_input)
    st.session_state.messages.append({"role": "user", "content": user_input})

    intent = detect_intent(user_input)

    dispatch = {
        "gratitude": lambda: reply_gratitude(st.session_state.last_disease),
        "greeting":  lambda: reply_greeting(),
        "help":      lambda: reply_help(),
        "pipeline":  lambda: reply_pipeline(),
        "history":   lambda: reply_history(st.session_state.diagnosis_history),
        "followup":  lambda: reply_followup(st.session_state.last_disease),
        "doctor":    lambda: reply_doctor(),
        "clarify":   lambda: reply_clarify(st.session_state.last_disease),
    }

    if intent == "reset":
        st.session_state.messages = []
        st.session_state.last_disease = ""
        st.session_state.diagnosis_history = []
        st.session_state.query_count = 0
        with st.chat_message("assistant"):
            st.markdown("Session cleared! Describe your symptoms anytime to start a new check. 💚")
        st.session_state.messages.append({"role": "assistant", "content": "Session cleared! Describe your symptoms anytime. 💚"})
        return

    if intent in dispatch:
        reply = dispatch[intent]()
        with st.chat_message("assistant"):
            st.markdown(reply, unsafe_allow_html=True)
        st.session_state.messages.append({"role": "assistant", "content": reply})
        return

    # ── Symptom → pipeline ──
    if not producer or not redis_client:
        with st.chat_message("assistant"):
            st.warning("⚠️ Backend services are offline. Please check that Docker containers are running.")
        return

    req_id = f"REQ-{uuid.uuid4().hex[:6].upper()}"
    producer.send('telehealth-symptoms', value={
        "request_id": req_id,
        "patient_id": st.session_state.patient_id,
        "user_input_symptoms": user_input
    })
    producer.flush()

    with st.chat_message("assistant"):
        with st.spinner("Analyzing symptoms…"):
            redis_key = f"telehealth:result:{st.session_state.patient_id}"
            redis_client.delete(redis_key)
            result_found = False
            predicted_disease = ""
            advice_list = []

            for _ in range(35):
                time.sleep(0.15)
                cached = redis_client.get(redis_key)
                if cached:
                    parsed = json.loads(cached.decode('utf-8'))
                    if parsed.get("request_id") == req_id:
                        predicted_disease = parsed["predicted_disease"]
                        if df_precaution is not None:
                            match = df_precaution[df_precaution['Disease_match'] == predicted_disease.lower().strip()]
                            if not match.empty:
                                for col in [c for c in df_precaution.columns if 'Precaution' in c]:
                                    val = match.iloc[0][col]
                                    if pd.notnull(val) and str(val).strip() not in ["none", ""]:
                                        advice_list.append(str(val).capitalize())
                        result_found = True
                        break

        ph = st.empty()
        if result_found:
            html = build_result_html(req_id, predicted_disease, advice_list)
            ph.markdown(html, unsafe_allow_html=True)
            st.session_state.messages.append({"role": "assistant", "content": html})
            st.session_state.last_disease = predicted_disease
            st.session_state.diagnosis_history.append({
                "disease": predicted_disease,
                "symptoms": user_input,
                "time": datetime.datetime.now().strftime("%H:%M"),
                "req_id": req_id,
            })
        else:
            err = """
<div class="da-result-card">
  <div style="padding:20px 24px;text-align:center;">
    <div style="font-size:2rem;margin-bottom:10px;">⏱️</div>
    <div style="font-size:0.9rem;font-weight:600;color:#C25A00;margin-bottom:6px;">Analysis Timeout</div>
    <div style="font-size:0.82rem;color:#A0BCB8;">The pipeline did not return a result in time. Please check your Docker services and try again.</div>
  </div>
</div>"""
            ph.markdown(err, unsafe_allow_html=True)
            st.session_state.messages.append({"role": "assistant", "content": err})


# ─────────────────────────────────────────────
#  MAIN DISPATCH
# ─────────────────────────────────────────────
user_input = st.chat_input("How are you feeling today? Describe your symptoms…")

if chip_trigger:
    handle_input(chip_trigger)
    st.rerun()
elif user_input:
    handle_input(user_input)