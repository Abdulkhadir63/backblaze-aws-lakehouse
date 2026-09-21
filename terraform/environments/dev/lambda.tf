resource "aws_lambda_function" "event_handler" {
  function_name = "backblaze-dev-s3-event-handler"

  runtime = "python3.13"
  handler = "lambda_function.lambda_handler"

  role = "arn:aws:iam::131912110087:role/service-role/backblaze-dev-s3-event-handler-role-tmlsc7z7"

  package_type = "Zip"

  s3_bucket = aws_s3_bucket.deployment_artifacts.bucket

  s3_key = "lambda/backblaze-dev-s3-event-handler/bd08c4c71b57cf40e4c861db734994655cb6e155.zip"

  s3_object_version = "k6_2FhMasBIGHsRkEzBC9AlupsLGnDCM"

  architectures = ["x86_64"]

  memory_size = 256
  timeout     = 10

  ephemeral_storage {
    size = 512
  }

  tracing_config {
    mode = "PassThrough"
  }

  logging_config {
    log_format = "Text"
    log_group  = "/aws/lambda/backblaze-dev-s3-event-handler"
  }

  lifecycle {
    ignore_changes = [
      s3_key,
      s3_object_version,
      source_code_hash,
      publish
    ]
  }
}