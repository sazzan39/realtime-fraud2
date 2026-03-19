import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
import os
import time

# Load model
model = joblib.load("model/random_forest_model.joblib")

# Feature order 
FEATURES = ['Time','V1','V2','V3','V4','V5','V6','V7','V8','V9',
            'V10','V11','V12','V13','V14','V15','V16','V17','V18',
            'V19','V20','V21','V22','V23','V24','V25','V26','V27',
            'V28','Amount']

QUEUE_FILE = "streaming/transactions_queue.jsonl"

# Create results file
if not os.path.exists("results.csv"):
    with open("results.csv", "w") as f:
        pass


consumer = None

try:
    consumer = KafkaConsumer(
        "transactions",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
except Exception:
    consumer = None

if consumer:
    print("Consumer started in Kafka mode...")
else:
    print("Consumer started in file-queue mode...")


def handle_transaction(transaction):
    try:
        if transaction is None:
            return

        # Remove label
        if "Class" in transaction:
            transaction.pop("Class")

       
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


if consumer:
    for msg in consumer:
        handle_transaction(msg.value)
else:
    open(QUEUE_FILE, "a", encoding="utf-8").close()
    offset = 0

    while True:
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            f.seek(offset)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                handle_transaction(json.loads(line))
            offset = f.tell()
        time.sleep(0.2)