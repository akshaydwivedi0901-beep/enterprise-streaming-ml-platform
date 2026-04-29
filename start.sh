#!/bin/bash
echo "🚀 Starting Enterprise Streaming Platform..."

echo "1️⃣  Starting Zookeeper + Kafka..."
docker-compose up -d zookeeper kafka

echo "⏳ Waiting 40s for Kafka to be healthy..."
sleep 40

# Verify kafka is healthy before proceeding
KAFKA_STATUS=$(docker inspect kafka --format='{{.State.Health.Status}}' 2>/dev/null)
if [ "$KAFKA_STATUS" != "healthy" ]; then
    echo "⚠️  Kafka not healthy yet, waiting 20 more seconds..."
    sleep 20
fi

echo "2️⃣  Starting Spark..."
docker-compose up -d spark spark-worker
sleep 10

echo "3️⃣  Starting Producer + Consumer..."
docker-compose up -d producer consumer

echo "4️⃣  Starting Monitoring..."
docker-compose up -d prometheus grafana

echo "5️⃣  Starting Airflow (db init is automatic)..."
docker-compose up -d airflow-webserver airflow-scheduler

echo "⏳ Waiting 45s for Airflow to initialize..."
sleep 45

echo "4️⃣  All services status:"
docker ps --format "table {{.Names}}\t{{.Status}}"

echo ""
echo "✅ Done! Open these URLs in PORTS tab:"
echo "   Airflow  → port 8081 (admin/admin)"
echo "   Spark    → port 8080"
echo "   Grafana  → port 3000 (admin/admin)"
echo "   Metrics  → port 8000/metrics"
