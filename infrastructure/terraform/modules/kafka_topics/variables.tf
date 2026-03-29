variable "topics" {
  description = "Kafka topics definition"
  type = map(object({
    partitions         = number
    replication_factor = number
    retention_ms       = string
  }))
}
