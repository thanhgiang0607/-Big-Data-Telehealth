import os
import pickle
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, classification_report, confusion_matrix
from sklearn.metrics.pairwise import cosine_similarity

sns.set_theme(style="white")
plt.rcParams.update({'font.size': 10, 'axes.labelsize': 12, 'axes.titlesize': 14})

CLEANED_DATA_PATH = 'data/cleaned/cleaned_dataset.csv'
MODEL_DIR = 'models/'

COSINE_MIN = 0.25
COSINE_MAX = 0.65

def smart_display(df):
    try:
        from IPython.display import display
        display(df)
    except ImportError:
        print("\n", df.to_string(index=False))

print("==================================================================")
print("📊 EVALUATION METRICS REPORT & CLINICAL INFERENCE TESTING")
print("==================================================================")

try:
    with open(os.path.join(MODEL_DIR, 'sbert_model.pkl'), 'rb') as f: sbert_model = pickle.load(f)
    with open(os.path.join(MODEL_DIR, 'label_encoder.pkl'), 'rb') as f: le = pickle.load(f)
    with open(os.path.join(MODEL_DIR, 'train_embeddings.pkl'), 'rb') as f: train_data = pickle.load(f)
    X_train = train_data['X_train']
    y_train = train_data['y_train']
    print("✔️ Successfully loaded Core Components and Train Embeddings!")
except FileNotFoundError:
    print("❌ Cannot find model files. Run scripts/train_pipeline.py first!")
    exit()

df_clean = pd.read_csv(CLEANED_DATA_PATH)
df_clean['Cleaned_Symptoms_Text'] = df_clean['Cleaned_Symptoms_Text'].astype(str).fillna('none')
df_clean['disease_label'] = le.transform(df_clean['Disease'])

X_embeddings = sbert_model.encode(df_clean['Cleaned_Symptoms_Text'].tolist(), show_progress_bar=False, convert_to_numpy=True)
y_labels = df_clean['disease_label'].values

from sklearn.model_selection import train_test_split
_, X_test, _, y_test = train_test_split(
    X_embeddings, y_labels, test_size=0.2, random_state=42, stratify=y_labels
)

y_pred_minmax = []
for test_vector in X_test:
    raw_similarities = cosine_similarity([test_vector], X_train)[0]
    best_match_idx = np.argmax(raw_similarities)
    y_pred_minmax.append(y_train[best_match_idx])

# ==================================================================
# SECTION 1: GLOBAL METRICS SUMMARY TABLE
# ==================================================================
print("\n📋 SECTION 1: GLOBAL METRICS SUMMARY TABLE")
print("-" * 75)
acc = accuracy_score(y_test, y_pred_minmax)
macro_precision, macro_recall, macro_f1, _ = precision_recall_fscore_support(y_test, y_pred_minmax, average='macro')

metrics_summary = {
    "Evaluation Metric": ["Accuracy", "Macro Average Precision", "Macro Average Recall", "Macro Average F1-Score"],
    "Value Score": [f"{acc*100:.2f}%", f"{macro_precision*100:.2f}%", f"{macro_recall*100:.2f}%", f"{macro_f1*100:.2f}%"],
    "Statistical Standard": ["Global Core Performance", "Unweighted Class Quality", "Unweighted System Recall", "Robustness Balance Indicator"]
}
smart_display(pd.DataFrame(metrics_summary))

# ==================================================================
# SECTION 2: PER-CLASS DETAILED PERFORMANCE MATRIX
# ==================================================================
print("\n📝 SECTION 2: PER-CLASS DETAILED PERFORMANCE MATRIX")
print("-" * 75)
print(classification_report(y_test, y_pred_minmax, target_names=le.classes_))

# ==================================================================
# SECTION 3: CONFUSION MATRIX HEATMAP GENERATION
# ==================================================================
print("\n🎨 SECTION 3: GENERATING AI ENGINE PERFORMANCE CONFUSION MATRIX")
print("-" * 75)
cm = confusion_matrix(y_test, y_pred_minmax)

unique_test_labels = np.unique(y_test)
cm_filtered = cm[unique_test_labels][:, unique_test_labels]
class_names_filtered = [str(name).title() for name in le.inverse_transform(unique_test_labels)]

plt.figure(figsize=(14, 11))
sns.heatmap(
    cm_filtered, annot=True, fmt='d', cmap='Blues',
    xticklabels=class_names_filtered, yticklabels=class_names_filtered,
    linewidths=0.5, linecolor='silver', cbar_kws={"label": "Number of Classified Instances"}, square=True
)

plt.title('AI Engine Performance: Confusion Matrix Heatmap', fontweight='bold', pad=20, fontsize=16)
plt.xlabel('Predicted Disease Class By SBERT Engine', fontweight='bold', labelpad=15)
plt.ylabel('Actual True Disease Class (Ground Truth)', fontweight='bold', labelpad=15)
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()

output_image_path = 'data/cleaned/ai_confusion_matrix.png'
plt.savefig(output_image_path, dpi=300)
print(f"💾 High-resolution chart saved to: {output_image_path}")

# ==================================================================
# SECTION 4: CLINICAL NATURAL LANGUAGE TESTING
# ==================================================================
print("\n📡 SECTION 4: STREAMING NATURAL LANGUAGE CLINICAL TEST CASES")
print("-" * 75)

test_cases_free = [
    {"Case_ID": "TC-001", "Scenario": "Heavy itching with visible red spots.", "User_Speech": "My skin feels incredibly itchy and I noticed some strange red rash and spots popping up on my body."},
    {"Case_ID": "TC-002", "Scenario": "Continuous sneezing and cold shivering.", "User_Speech": "I cannot stop sneezing since this morning, my whole body is shivering and shaking because it is too cold."},
    {"Case_ID": "TC-003", "Scenario": "Severe jaundice indicators with abdominal discomfort.", "User_Speech": "Suddenly suffering from vomiting and a very clear yellowish skin color tone, also feeling pain in my upper abdomen."},
    {"Case_ID": "TC-004", "Scenario": "Elderly patient suffering from severe joint pain.", "User_Speech": "My knee joints are swelling terribly, causing acute pain and stiff struggles whenever I try walking."},
    {"Case_ID": "TC-005", "Scenario": "Emergency cardiac arrest/heart attack symptoms.", "User_Speech": "Suddenly feeling an intense heavy chest pain, I am struggling to breathe right now and sweating cold sweat."}
]

for case in test_cases_free:
    user_input = str(case["User_Speech"]).lower().strip().replace('_', ' ')
    user_vector = sbert_model.encode([user_input])

    raw_similarities = cosine_similarity(user_vector, X_train)[0]
    best_match_idx = np.argmax(raw_similarities)
    best_raw_cosine = raw_similarities[best_match_idx]

    predicted_label_code = y_train[best_match_idx]
    predicted_disease_name = le.inverse_transform([predicted_label_code])[0]

    clipped_cosine = np.clip(best_raw_cosine, COSINE_MIN, COSINE_MAX)
    calibrated_confidence = ((clipped_cosine - COSINE_MIN) / (COSINE_MAX - COSINE_MIN)) * 100

    print(f"[CASE ID: {case['Case_ID']}]")
    print(f"   + Clinical Target: {case['Scenario']}")
    print(f"   + Patient Said   : \"{case['User_Speech']}\"")
    print(f"   💥 AI DIAGNOSIS   : {str(predicted_disease_name).title()}")
    print(f"      CONFIDENCE    : {calibrated_confidence:.2f}% Semantic Match (Raw Cosine: {best_raw_cosine:.4f})")
    print("-" * 75)

print('🎉 [ALL PIPELINE REPORTS GENERATED COMPLETED!]')