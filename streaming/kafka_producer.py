import pandas as pd
import json
import time
import random
from kafka import KafkaProducer

data = pd.read_csv("datasets/creditcard.csv")

fraud = data[data["Class"] == 1]
normal = data[data["Class"] == 0]

QUEUE_FILE = "streaming/transactions_queue.jsonl"

producer = None

try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
except Exception:
    producer = None

if producer:
    print("Producer started in Kafka mode...")
else:
    print("Producer started in file-queue mode...")

while True:
    row = fraud.sample() if random.random() < 0.2 else normal.sample()
    txn = row.to_dict(orient="records")[0]

    if producer:
        producer.send("transactions", txn)
    else:
        with open(QUEUE_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(txn) + "\n")

    print("Transaction sent")

    time.sleep(0.05)   