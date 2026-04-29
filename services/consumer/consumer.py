# ============================================================================
# FILE: services/consumer/consumer.py
# COMPLETE KAFKA CONSUMER with fraud detection, data quality, and monitoring
# Copy entire file to: services/consumer/consumer.py
# ============================================================================

import os
import json
import logging
import signal
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Any, List, Optional

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from jsonschema import validate, ValidationError
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from tenacity import retry, wait_exponential, stop_after_attempt


# ============================================================================
# CONFIGURATION
# ============================================================================

# Logging setup
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
)
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "user-event-processor-group")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "enterprise-streaming-dev-bronze")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
FRAUD_SCORE_THRESHOLD = float(os.getenv("FRAUD_SCORE_THRESHOLD", "0.8"))
FRAUD_VELOCITY_THRESHOLD = int(os.getenv("FRAUD_VELOCITY_THRESHOLD", "5"))


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

# Counters
events_processed_total = Counter(
    "events_processed_total",
    "Total events successfully processed",
    labelnames=["status"]
)
events_failed_total = Counter(
    "events_failed_total",
    "Total events that failed processing",
    labelnames=["error_type"]
)
dlq_events_total = Counter(
    "dlq_events_total",
    "Total events sent to dead letter queue"
)
s3_upload_errors_total = Counter(
    "s3_upload_errors_total",
    "Total S3 upload failures",
    labelnames=["error_type"]
)

# Gauges
consumer_lag = Gauge(
    "consumer_lag",
    "Current consumer lag in messages"
)
events_in_dlq = Gauge(
    "events_in_dlq",
    "Current size of dead letter queue"
)
consumer_offset = Gauge(
    "consumer_offset",
    "Current consumer offset",
    labelnames=["partition"]
)

# Histograms
event_processing_time = Histogram(
    "event_processing_time_seconds",
    "Time to process a single event",
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 5.0)
)
fraud_score_distribution = Histogram(
    "fraud_score_distribution",
    "Distribution of fraud scores",
    buckets=(0.1, 0.2, 0.3, 0.5, 0.7, 0.8, 0.9, 1.0)
)
s3_upload_duration = Histogram(
    "s3_upload_duration_seconds",
    "Time to upload to S3"
)

# Event tracking
events_by_country = Counter(
    "events_by_country",
    "Events processed by country",
    labelnames=["country"]
)
events_by_device = Counter(
    "events_by_device",
    "Events processed by device type",
    labelnames=["device_type"]
)
velocity_distribution = Histogram(
    "user_velocity_10min",
    "Distribution of user transaction velocity in 10min window",
    buckets=(1, 2, 3, 5, 10, 20)
)


# ============================================================================
# JSON SCHEMA VALIDATION
# ============================================================================

EVENT_SCHEMA = {
    "type": "object",
    "required": ["event_id", "user_id", "amount", "device_type", "country", "timestamp"],
    "properties": {
        "event_id": {"type": "string", "minLength": 1},
        "user_id": {"type": "string", "minLength": 1},
        "amount": {"type": "number", "minimum": 0},
        "device_type": {"type": "string", "enum": ["mobile", "web", "tablet"]},
        "country": {"type": "string", "minLength": 2, "maxLength": 3},
        "timestamp": {"type": "string"},  # ISO 8601
    },
    "additionalProperties": False,
}


# ============================================================================
# AWS S3 CLIENT
# ============================================================================

class S3Client:
    """Wrapper for S3 operations with retry logic and error handling"""
    
    def __init__(self, region_name: str):
        self.region_name = region_name
        self.client = boto3.client("s3", region_name=region_name)
    
    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True
    )
    def put_object(self, bucket: str, key: str, body: bytes) -> bool:
        """
        Upload object to S3 with exponential backoff retry.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            body: Object content as bytes
            
        Returns:
            True if successful
            
        Raises:
            ClientError: If S3 operation fails after retries
        """
        try:
            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ServerSideEncryption="AES256",  # Enable encryption
                ContentType="application/json"
            )
            logger.debug(f"Successfully uploaded to s3://{bucket}/{key}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"S3 upload failed for {bucket}/{key}: {str(e)}")
            raise


# ============================================================================
# FRAUD DETECTION ENGINE
# ============================================================================

class FraudDetectionEngine:
    """Stateful fraud detection using velocity and simple rules"""
    
    def __init__(self, velocity_threshold: int = 5, window_minutes: int = 10):
        self.velocity_threshold = velocity_threshold
        self.window_minutes = window_minutes
        self.user_state: Dict[str, List[datetime]] = defaultdict(list)
    
    def compute_velocity(self, user_id: str) -> int:
        """
        Compute transaction velocity for a user in the last N minutes.
        
        Args:
            user_id: User identifier
            
        Returns:
            Number of transactions in the time window
        """
        now = datetime.utcnow()
        window_start = now - timedelta(minutes=self.window_minutes)
        
        # Remove old transactions outside the window
        self.user_state[user_id] = [
            t for t in self.user_state[user_id] if t > window_start
        ]
        
        # Add current transaction
        self.user_state[user_id].append(now)
        
        velocity = len(self.user_state[user_id])
        logger.debug(f"User {user_id}: velocity={velocity}")
        return velocity
    
    def score_fraud(self, event: Dict[str, Any]) -> tuple[float, str]:
        """
        Calculate fraud score and return reason.
        
        Args:
            event: Event data
            
        Returns:
            Tuple of (fraud_score, reason)
        """
        user_id = event.get("user_id")
        amount = float(event.get("amount", 0))
        
        # Check velocity
        velocity = self.compute_velocity(user_id)
        if velocity > self.velocity_threshold:
            return 0.9, f"High velocity: {velocity} transactions in {self.window_minutes}m"
        
        # Amount-based scoring
        fraud_score = min(amount * 0.001, 0.5)  # 0-0.5 based on amount
        
        return fraud_score, "Amount-based scoring"


# ============================================================================
# MAIN CONSUMER CLASS
# ============================================================================

class StreamingConsumer:
    """Kafka consumer for fraud detection and event enrichment"""
    
    def __init__(self):
        self.s3 = S3Client(AWS_REGION)
        self.fraud_engine = FraudDetectionEngine(
            velocity_threshold=FRAUD_VELOCITY_THRESHOLD
        )
        self.running = True
        self.dlq_count = 0
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
    
    def _shutdown_handler(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.running = False
    
    def create_consumer(self) -> KafkaConsumer:
        """Create and configure Kafka consumer"""
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: m,
            session_timeout_ms=30000,
            max_poll_records=100,
            connections_max_idle_ms=540000,
        )
        logger.info(f"Kafka consumer created for topic '{KAFKA_TOPIC}'")
        return consumer
    
    def validate_event(self, event_data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate event against schema.
        
        Args:
            event_data: Event dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            validate(instance=event_data, schema=EVENT_SCHEMA)
            return True, None
        except ValidationError as e:
            error_msg = f"Validation failed: {e.message}"
            logger.warning(error_msg)
            return False, error_msg
    
    def save_to_s3(self, prefix: str, event: Dict[str, Any]) -> bool:
        """
        Save event to S3 with partitioning by date.
        
        Args:
            prefix: S3 prefix (e.g., 'bronze/enriched')
            event: Event data
            
        Returns:
            True if successful
        """
        try:
            now = datetime.utcnow()
            key = (
                f"{prefix}/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                f"{event['event_id']}.json"
            )
            
            body = json.dumps(event).encode("utf-8")
            
            import time
            start = time.time()
            self.s3.put_object(BRONZE_BUCKET, key, body)
            duration = time.time() - start
            s3_upload_duration.observe(duration)
            
            logger.debug(f"Event {event['event_id']} saved to {prefix}")
            return True
        
        except Exception as e:
            error_type = type(e).__name__
            s3_upload_errors_total.labels(error_type=error_type).inc()
            logger.error(f"Failed to save event {event.get('event_id')}: {str(e)}")
            return False
    
    def process_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Process single event: validate, enrich, detect fraud, store.
        
        Args:
            event_data: Raw event from Kafka
            
        Returns:
            True if successful
        """
        import time
        start_time = time.time()
        
        try:
            # 1. Validate schema
            is_valid, error_msg = self.validate_event(event_data)
            if not is_valid:
                dlq_events_total.inc()
                self.save_to_s3("bronze/dlq", event_data)
                events_failed_total.labels(error_type="schema_validation").inc()
                logger.warning(f"Event {event_data.get('event_id')} rejected: {error_msg}")
                return False
            
            # 2. Fraud detection
            fraud_score, reason = self.fraud_engine.score_fraud(event_data)
            event_data["fraud_score"] = fraud_score
            event_data["fraud_reason"] = reason
            
            # 3. Enrichment
            event_data["processed_at"] = datetime.utcnow().isoformat()
            event_data["velocity"] = len(
                self.fraud_engine.user_state.get(event_data["user_id"], [])
            )
            
            # 4. Record metrics
            fraud_score_distribution.observe(fraud_score)
            events_by_country.labels(country=event_data["country"]).inc()
            events_by_device.labels(device_type=event_data["device_type"]).inc()
            velocity_distribution.observe(event_data["velocity"])
            
            # 5. Save enriched event
            self.save_to_s3("bronze/enriched", event_data)
            
            # 6. Alert on high fraud
            if fraud_score >= FRAUD_SCORE_THRESHOLD:
                self.save_to_s3("bronze/fraud_alerts", event_data)
                logger.warning(
                    f"High fraud score detected: {fraud_score} for user {event_data['user_id']}"
                )
            
            # 7. Record success
            events_processed_total.labels(status="success").inc()
            
            # 8. Record timing
            duration = time.time() - start_time
            event_processing_time.observe(duration)
            
            logger.debug(f"Event {event_data['event_id']} processed successfully in {duration:.3f}s")
            return True
        
        except Exception as e:
            error_type = type(e).__name__
            events_failed_total.labels(error_type=error_type).inc()
            logger.error(f"Error processing event: {str(e)}", exc_info=True)
            return False
    
    def update_lag_metrics(self, consumer: KafkaConsumer):
        """Update consumer lag and offset metrics"""
        try:
            for tp in consumer.assignment():
                committed = consumer.committed(tp)
                end = consumer.end_offsets([tp])[tp]
                
                if committed:
                    lag = end - committed
                    consumer_lag.set(lag)
                
                consumer_offset.labels(partition=tp.partition).set(end)
        
        except Exception as e:
            logger.warning(f"Failed to update lag metrics: {str(e)}")
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting Kafka consumer...")
        
        # Start Prometheus metrics server
        start_http_server(8000)
        logger.info("Prometheus metrics server started on port 8000")
        
        consumer = self.create_consumer()
        
        try:
            message_count = 0
            while self.running:
                messages = consumer.poll(timeout_ms=1000, max_records=100)
                
                if not messages:
                    # Update metrics even with no messages
                    self.update_lag_metrics(consumer)
                    events_in_dlq.set(self.dlq_count)
                    continue
                
                for topic_partition, records in messages.items():
                    for message in records:
                        try:
                            # Deserialize
                            event = json.loads(message.value.decode("utf-8"))
                            
                            # Process
                            success = self.process_event(event)
                            
                            # Commit offset only after successful processing
                            if success:
                                consumer.commit()
                                message_count += 1
                                if message_count % 100 == 0:
                                    logger.info(f"Processed {message_count} events")
                        
                        except json.JSONDecodeError as e:
                            events_failed_total.labels(error_type="json_decode").inc()
                            logger.error(f"Failed to deserialize message: {str(e)}")
                        except Exception as e:
                            events_failed_total.labels(error_type="unknown").inc()
                            logger.error(f"Unexpected error: {str(e)}", exc_info=True)
                
                # Update metrics
                self.update_lag_metrics(consumer)
                events_in_dlq.set(self.dlq_count)
        
        except KafkaError as e:
            logger.error(f"Kafka error: {str(e)}", exc_info=True)
            raise
        
        finally:
            consumer.close()
            logger.info(f"Consumer closed. Processed {message_count} total events.")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        consumer = StreamingConsumer()
        consumer.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)