import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
import csv
import os

# Load model
model = joblib.load("model/fraud_model.pkl")

# Feature order (VERY IMPORTANT)
FEATURES = ['Time','V1','V2','V3','V4','V5','V6','V7','V8','V9',
            'V10','V11','V12','V13','V14','V15','V16','V17','V18',
            'V19','V20','V21','V22','V23','V24','V25','V26','V27',
            'V28','Amount']

# Create results file
if not os.path.exists("results.csv"):
    with open("results.csv", "w") as f:
        pass

# Kafka consumer
consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("🚀 Consumer started...")

for msg in consumer:
    try:
        transaction = msg.value

        # Remove label
        if "Class" in transaction:
            transaction.pop("Class")

        # Convert to DataFrame
        X = pd.DataFrame([transaction])
        X = X[FEATURES]

        # Predict
        prob = model.predict_proba(X)[0][1]
        status = "FRAUD" if prob > 0.5 else "NORMAL"

        # Save result (fast write)
        pd.DataFrame([[prob, status]]).to_csv(
            "results.csv",
            mode="a",
            header=False,
            index=False
        )

        print(f"{status} | {prob:.2f}")

    except Exception as e:
        print("Consumer Error:", e)