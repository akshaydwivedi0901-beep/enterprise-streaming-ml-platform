from kafka import KafkaProducer
import json
import uuid
import random
from datetime import datetime
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
)

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1,100)}",  # smaller range for velocity testing
        "country": random.choice(["USA", "India", "Germany"]),
        "amount": round(random.uniform(10, 500), 2),
        "device_type": random.choice(["mobile", "web", "tablet"]),
        "timestamp": datetime.utcnow().isoformat()
    }

while True:
    event = generate_event()

    producer.send(
        "user-events",
        key=event["user_id"],   # 🔥 CRITICAL FIX: ensures same user → same partition
        value=event
    )

    print("Produced:", event)
    time.sleep(0.05)