import os

RAW_DATA_DIR = "data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

print("🚀 Đang dùng Kaggle CLI kéo dữ liệu trực tiếp về thư mục data/raw...")
print("-" * 60)

# Dataset 1: Disease Symptom
print("📥 Tải Dataset 1: Disease Symptom...")
os.system(f"kaggle datasets download -d itachi9604/disease-symptom-description-dataset -p {RAW_DATA_DIR} --unzip")

# Dataset 2: Patient records
print("\n📥 Tải Dataset 2: Patient records...")
os.system(f"kaggle datasets download -d prasad22/healthcare-dataset -p {RAW_DATA_DIR} --unzip")

# Dataset 3: History
print("\n📥 Tải Dataset 3: Hospital Data...")
os.system(f"kaggle datasets download -d blueblushed/hospital-dataset-for-practice -p {RAW_DATA_DIR} --unzip")

print("-" * 60)
print("🎉 HOÀN THÀNH CHÍNH THỨC TỪ KAGGLE! Các file hiện có:")
print(os.listdir(RAW_DATA_DIR))