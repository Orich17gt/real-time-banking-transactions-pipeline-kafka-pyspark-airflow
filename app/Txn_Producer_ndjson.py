from kafka import KafkaProducer
import json
import time
import os

# ---------------------------
# CONFIG
# ---------------------------
KAFKA_BROKER = "kafka:9092"
TOPIC = "txn"
NDJSON_DIR = "/data/raw/"   # <-- update your directory

# ---------------------------
# Kafka Producer
# ---------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    linger_ms=5,
    batch_size=16384,
    acks="all"
)

record_count = 0
start_time = time.time()

print("📥 Streaming TRANSACTION data to Kafka topic 'txn'...\n")

files = sorted([f for f in os.listdir(NDJSON_DIR) if f.endswith(".ndjson")])

for file_name in files:
    file_path = os.path.join(NDJSON_DIR, file_name)
    print(f"📂 Processing file: {file_name}")

    with open(file_path, "r") as f:
        for line in f:
            record = json.loads(line.strip())

            # ---------------------------
            # Key for partitioning
            # ---------------------------
            key = record["account_id"]

            # ---------------------------
            # Send to Kafka
            # ---------------------------
            producer.send(
                TOPIC,
                key=key,
                value=record
            )

            record_count += 1

            if record_count % 10000 == 0:
                elapsed = time.time() - start_time
                print(f"📊 Sent {record_count:,} records | {record_count/elapsed:.2f} rec/sec")

producer.flush()

total_time = time.time() - start_time

print("\n✅ All TRANSACTION messages sent")
print(f"📦 Total records: {record_count:,}")
print(f"🚀 Throughput: {record_count/total_time:.2f} rec/sec")