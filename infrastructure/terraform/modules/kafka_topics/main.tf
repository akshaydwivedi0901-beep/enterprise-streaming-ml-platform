terraform {
  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "~> 0.5"
    }
  }
}

provider "kafka" {
  bootstrap_servers = ["localhost:9092"]
}

resource "kafka_topic" "topics" {
  for_each = var.topics

  name               = each.key
  partitions         = each.value.partitions
  replication_factor = each.value.replication_factor

  config = {
    "retention.ms" = each.value.retention_ms
  }
}
