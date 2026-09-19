resource "aws_lambda_event_source_mapping" "sqs" {
  event_source_arn                   = aws_sqs_queue.main.arn
  function_name                      = "backblaze-dev-s3-event-handler"
  batch_size                         = 1
  maximum_batching_window_in_seconds = 0
  enabled                            = true

  lifecycle {
    ignore_changes = [
      metrics_config
    ]
  }
}
