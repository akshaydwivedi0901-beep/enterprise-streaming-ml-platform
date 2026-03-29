output "topic_names" {
  description = "Created Kafka topic names"
  value       = [for t in kafka_topic.topics : t.name]
}
