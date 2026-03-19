import pandas as pd
import json
import time
import random
from kafka import KafkaProducer

data = pd.read_csv("datasets/creditcard.csv")

fraud = data[data["Class"] == 1]
normal = data[data["Class"] == 0]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚀 Producer started...")

while True:
    row = fraud.sample() if random.random() < 0.2 else normal.sample()
    txn = row.to_dict(orient="records")[0]

    producer.send("transactions", txn)

    print("→ sent")

    time.sleep(0.05)   # ⚡ FAST (20/sec)