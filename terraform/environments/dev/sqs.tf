resource "aws_sqs_queue" "dlq" {
  name = "backblaze-dev-s3-events-dlq"

  visibility_timeout_seconds = 60
  message_retention_seconds  = 604800
  delay_seconds              = 0
  receive_wait_time_seconds  = 20
  max_message_size           = 1048576
  sqs_managed_sse_enabled    = true
}

resource "aws_sqs_queue" "main" {
  name = "backblaze-dev-s3-events"

  visibility_timeout_seconds = 60
  message_retention_seconds  = 604800
  delay_seconds              = 0
  receive_wait_time_seconds  = 20
  max_message_size           = 1048576
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}
