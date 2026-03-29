import os
import json
import logging
import signal
from datetime import datetime, timedelta
from collections import defaultdict

import boto3
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from jsonschema import validate, ValidationError
from prometheus_client import start_http_server, Counter, Gauge

# ==================================================
# Logging
# ==================================================
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
)
logger = logging.getLogger(__name__)

# ==================================================
# Environment Variables
# ==================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "user-event-processor-group")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "enterprise-streaming-dev-bronze")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# ==================================================
# Prometheus Metrics
# ==================================================
events_processed_total = Counter("events_processed_total", "Total events processed")
events_failed_total = Counter("events_failed_total", "Total failed events")
dlq_events_total = Counter("dlq_events_total", "Total DLQ events")
consumer_lag = Gauge("consumer_lag", "Current consumer lag")

# ==================================================
# JSON Schema
# ==================================================
EVENT_SCHEMA = {
    "type": "object",
    "required": ["event_id", "user_id", "amount", "device_type", "country", "timestamp"],
    "properties": {
        "event_id": {"type": "string"},
        "user_id": {"type": "string"},
        "amount": {"type": "number"},
        "device_type": {"type": "string"},
        "country": {"type": "string"},
        "timestamp": {"type": "string"},
    },
}

# ==================================================
# AWS S3 Client
# ==================================================
s3 = boto3.client("s3", region_name=AWS_REGION)

# ==================================================
# Stateful Fraud Tracking (Per Partition Safe)
# ==================================================
user_state = defaultdict(list)
FRAUD_THRESHOLD = 0.8

def compute_velocity(user_id):
    now = datetime.utcnow()
    history = user_state[user_id]

    # Remove transactions older than 10 minutes
    history = [t for t in history if now - t < timedelta(minutes=10)]
    history.append(now)

    user_state[user_id] = history
    return len(history)

# ==================================================
# Save to S3
# ==================================================
def save_to_s3(prefix: str, event: dict):
    now = datetime.utcnow()

    key = (
        f"{prefix}/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{event['event_id']}.json"
    )

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(event).encode("utf-8"),
    )

# ==================================================
# Graceful Shutdown
# ==================================================
running = True

def shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received.")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ==================================================
# Main
# ==================================================
if __name__ == "__main__":

    start_http_server(8000)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: m,
    )

    logger.info("Fraud Consumer Started")

    try:
        while running:
            for message in consumer:

                if not message.value:
                    continue

                try:
                    event = json.loads(message.value.decode("utf-8"))
                except json.JSONDecodeError:
                    events_failed_total.inc()
                    continue

                # Schema validation
                try:
                    validate(instance=event, schema=EVENT_SCHEMA)
                except ValidationError:
                    dlq_events_total.inc()
                    save_to_s3("bronze/dlq", event)
                    consumer.commit()
                    continue

                # ==================================================
                # Stateful Fraud Logic
                # ==================================================
                velocity = compute_velocity(event["user_id"])
                event["velocity_10min"] = velocity

                # Simple fraud rule
                if velocity > 5:
                    fraud_score = 0.9
                else:
                    fraud_score = round(event["amount"] * 0.01, 2)

                event["fraud_score"] = fraud_score
                event["processed_at"] = datetime.utcnow().isoformat()

                # ==================================================
                # Store enriched
                # ==================================================
                save_to_s3("bronze/enriched", event)

                # Fraud alert routing
                if fraud_score >= FRAUD_THRESHOLD:
                    save_to_s3("bronze/fraud_alerts", event)

                events_processed_total.inc()

                # ==================================================
                # Update Consumer Lag Metric
                # ==================================================
                for tp in consumer.assignment():
                    committed = consumer.committed(tp)
                    end = consumer.end_offsets([tp])[tp]
                    if committed:
                        consumer_lag.set(end - committed)

                consumer.commit()

    except KafkaError as e:
        logger.error(f"Kafka error: {e}")

    finally:
        consumer.close()
        logger.info("Consumer closed.")