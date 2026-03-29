variable "stream_name" {
  type = string
}

variable "shard_count" {
  type    = number
  default = 1
}

variable "retention_period" {
  type    = number
  default = 24
}

variable "environment" {
  type = string
}
