import json
import joblib
import pandas as pd
import os

from pyspark.sql import SparkSession

model = joblib.load("model/X_train.csv")

FEATURES = ['Time','V1','V2','V3','V4','V5','V6','V7','V8','V9',
            'V10','V11','V12','V13','V14','V15','V16','V17','V18',
            'V19','V20','V21','V22','V23','V24','V25','V26','V27',
            'V28','Amount']

if not os.path.exists("results.csv"):
    open("results.csv", "w").close()

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark running...")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "50") \
    .load()

transactions = df.selectExpr("CAST(value AS STRING)")

def process_batch(batch_df, batch_id):

    rows = batch_df.limit(50).collect()  

    with open("results.csv", "a") as f:  

        for row in rows:
            try:
                txn = json.loads(row.value)

                if "Class" in txn:
                    txn.pop("Class")

                X = pd.DataFrame([txn])[FEATURES]

                prob = model.predict_proba(X)[0][1]
                status = "FRAUD" if prob > 0.5 else "NORMAL"

                f.write(f"{prob},{status}\n")

                print(status, f"{prob:.2f}")

            except Exception as e:
                print("Error:", e)

query = transactions.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="0.5 seconds") \
    .start()

query.awaitTermination()