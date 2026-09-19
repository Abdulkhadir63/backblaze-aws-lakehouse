resource "aws_sfn_state_machine" "file_processing" {
  name     = "backblaze-dev-file-processing"
  role_arn = "arn:aws:iam::131912110087:role/service-role/StepFunctions-backblaze-dev-file-processing-role-25buedb4p"
  type     = "STANDARD"

  definition = file("../../../stepfunctions/backblaze_file_processing.asl.json")

  publish = false

  tracing_configuration {
    enabled = false
  }

  logging_configuration {
    level                  = "OFF"
    include_execution_data = false
  }
}
