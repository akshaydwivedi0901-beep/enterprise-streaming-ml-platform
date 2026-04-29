from kafka import KafkaProducer
import json, uuid, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Starting producer...")
try:
    while True:
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1,100)}",
            "country": random.choice(["USA", "India", "Germany"]),
            "amount": round(random.uniform(10, 500), 2),
            "device_type": random.choice(["mobile", "web", "tablet"]),
            "timestamp": datetime.utcnow().isoformat()
        }
        producer.send("user-events", key=event["user_id"], value=event)
        print(f"✓ {event['event_id']}")
        time.sleep(0.5)
except KeyboardInterrupt:
    print("\nStopped")
    producer.close()
