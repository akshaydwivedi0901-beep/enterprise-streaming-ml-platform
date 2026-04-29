terraform {
  required_providers {
    kafka = {
      source  = "mongey/kafka"
      version = "~> 0.5"
    }
  }
}

provider "kafka" {
  bootstrap_servers = ["localhost:9092"]
}
