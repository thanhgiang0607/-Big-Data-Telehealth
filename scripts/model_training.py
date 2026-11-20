"""
Train and benchmark candidate inference engines for the symptom checker.

Candidates (all use the same Sentence-BERT embeddings):
  1. SBERT 1-NN        - cosine nearest neighbour over training embeddings (serving engine)
  2. SBERT Centroid    - cosine similarity to the mean embedding of each disease
  3. Random Forest     - RandomForestClassifier trained on SBERT embeddings

Every number printed here is measured; nothing is simulated.
  - Hold-out: stratified 80/20 split (random_state=42), same split used for serving artifacts
  - Robustness: 5-fold stratified cross-validation (mean +/- std)
  - Latency: per-query inference time measured separately for each engine
"""
import os
import json
import time
import pickle
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import accuracy_score, f1_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

CLEANED_DATA_PATH = 'data/cleaned/cleaned_dataset.csv'
MODEL_DIR = 'models/'
REPORT_PATH = os.path.join(MODEL_DIR, 'benchmark_report.json')
SEED = 42
os.makedirs(MODEL_DIR, exist_ok=True)

print("=" * 70)
print("MODEL TRAINING & BENCHMARK: SBERT RETRIEVAL vs. ML CLASSIFIER")
print("=" * 70)

if not os.path.exists(CLEANED_DATA_PATH):
    raise SystemExit(f"Cannot find '{CLEANED_DATA_PATH}'. Run scripts/data_valuation.py first.")

df = pd.read_csv(CLEANED_DATA_PATH)
df['Cleaned_Symptoms_Text'] = df['Cleaned_Symptoms_Text'].astype(str).fillna('none')
print(f"Dataset: {len(df)} unique symptom profiles, {df['Disease'].nunique()} diseases")

label_encoder = LabelEncoder()
y = label_encoder.fit_transform(df['Disease'])

print("Encoding symptom text with Sentence-BERT (all-MiniLM-L6-v2)...")
sbert_model = SentenceTransformer('all-MiniLM-L6-v2')
X = sbert_model.encode(df['Cleaned_Symptoms_Text'].tolist(), show_progress_bar=False, convert_to_numpy=True)


# ---------------- candidate engines ----------------
def fit_1nn(X_tr, y_tr):
    return {'X': X_tr, 'y': y_tr}

def predict_1nn(m, X_te):
    return m['y'][np.argmax(cosine_similarity(X_te, m['X']), axis=1)]

def fit_centroid(X_tr, y_tr):
    classes = np.unique(y_tr)
    return {'classes': classes, 'C': np.vstack([X_tr[y_tr == c].mean(axis=0) for c in classes])}

def predict_centroid(m, X_te):
    return m['classes'][np.argmax(cosine_similarity(X_te, m['C']), axis=1)]

def fit_rf(X_tr, y_tr):
    return RandomForestClassifier(n_estimators=100, random_state=SEED, n_jobs=-1).fit(X_tr, y_tr)

def predict_rf(m, X_te):
    return m.predict(X_te)

ENGINES = {
    'SBERT 1-NN (cosine)': (fit_1nn, predict_1nn),
    'SBERT Class Centroid (cosine)': (fit_centroid, predict_centroid),
    'Random Forest on SBERT embeddings': (fit_rf, predict_rf),
}


def per_query_latency_ms(predict_fn, model, X_te):
    """Median latency of single-query inference (excludes SBERT encoding, identical for all engines)."""
    times = []
    for v in X_te:
        t0 = time.perf_counter()
        predict_fn(model, v.reshape(1, -1))
        times.append((time.perf_counter() - t0) * 1000)
    return float(np.median(times))


# ---------------- hold-out evaluation ----------------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=SEED, stratify=y
)
print(f"Hold-out split: {len(X_train)} train / {len(X_test)} test")

# ---------------- 5-fold stratified CV ----------------
skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=SEED)

rows, fitted = [], {}
for name, (fit_fn, pred_fn) in ENGINES.items():
    model = fit_fn(X_train, y_train)
    fitted[name] = model
    y_pred = pred_fn(model, X_test)

    cv_f1 = []
    for tr_idx, te_idx in skf.split(X, y):
        m = fit_fn(X[tr_idx], y[tr_idx])
        cv_f1.append(f1_score(y[te_idx], pred_fn(m, X[te_idx]), average='macro'))

    rows.append({
        'engine': name,
        'holdout_accuracy': accuracy_score(y_test, y_pred),
        'holdout_macro_f1': f1_score(y_test, y_pred, average='macro'),
        'cv5_macro_f1_mean': float(np.mean(cv_f1)),
        'cv5_macro_f1_std': float(np.std(cv_f1)),
        'median_latency_ms': per_query_latency_ms(pred_fn, model, X_test),
    })

report = pd.DataFrame(rows).sort_values('cv5_macro_f1_mean', ascending=False).reset_index(drop=True)
print("\nBENCHMARK REPORT")
print(report.to_string(index=False, float_format=lambda v: f"{v:.4f}"))

best = report.iloc[0]['engine']
print(f"\nBest engine by 5-fold CV macro-F1: {best}")
print("Note: serving (spark_streaming.py / main_worker.py) currently uses SBERT 1-NN (cosine).")
print("Caveat: the dataset holds only templated symptom lists (304 unique rows after removing "
      "~94% duplicates), so near-perfect scores here do NOT reflect accuracy on real patient free text.")

with open(REPORT_PATH, 'w') as f:
    json.dump({'n_samples': int(len(df)), 'n_classes': int(df['Disease'].nunique()),
               'results': rows, 'best_by_cv_macro_f1': best}, f, indent=2)
print(f"Report saved to {REPORT_PATH}")

# ---------------- serialize serving artifacts ----------------
with open(os.path.join(MODEL_DIR, 'sbert_model.pkl'), 'wb') as f:
    pickle.dump(sbert_model, f)
with open(os.path.join(MODEL_DIR, 'label_encoder.pkl'), 'wb') as f:
    pickle.dump(label_encoder, f)
with open(os.path.join(MODEL_DIR, 'rf_model.pkl'), 'wb') as f:
    pickle.dump(fitted['Random Forest on SBERT embeddings'], f)
with open(os.path.join(MODEL_DIR, 'train_embeddings.pkl'), 'wb') as f:
    pickle.dump({'X_train': X_train, 'y_train': y_train}, f)
print("Artifacts saved to models/")
