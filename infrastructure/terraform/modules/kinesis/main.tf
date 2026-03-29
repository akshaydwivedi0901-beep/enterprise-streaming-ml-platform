resource "aws_kinesis_stream" "this" {
  name             = var.stream_name
  shard_count      = var.shard_count
  retention_period = var.retention_period

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  tags = {
    Environment = var.environment
    Project     = "enterprise-streaming-ml"
  }
}

resource "aws_cloudwatch_metric_alarm" "write_throttle_alarm" {
  alarm_name          = "${var.stream_name}-write-throttle"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "WriteProvisionedThroughputExceeded"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Sum"
  threshold           = 0

  dimensions = {
    StreamName = aws_kinesis_stream.this.name
  }
}
