module "bronze_bucket" {
  source      = "../../modules/s3_bronze"
  bucket_name = "enterprise-streaming-dev-bronze"
  environment = "dev"
}
module "kafka_topics" {
  source = "../../modules/kafka_topics"

  topics = {
    "user-events" = {
      partitions         = 3
      replication_factor = 1
      retention_ms       = "604800000" # 7 days
    }

    "user-events-dlq" = {
      partitions         = 1
      replication_factor = 1
      retention_ms       = "2592000000" # 30 days
    }
  }
}
