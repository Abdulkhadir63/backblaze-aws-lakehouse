resource "aws_sns_topic" "pipeline_notifications" {
  name           = "backblaze-dev-pipeline-notifications"
  tracing_config = "PassThrough"
}
