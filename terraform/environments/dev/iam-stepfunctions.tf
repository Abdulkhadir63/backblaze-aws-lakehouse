resource "aws_iam_role" "stepfunctions_file_processing" {
  name = "StepFunctions-backblaze-dev-file-processing-role-25buedb4p"
  path = "/service-role/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "states.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })

  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "stepfunctions_dynamodb_full_access" {
  role       = aws_iam_role.stepfunctions_file_processing.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

resource "aws_iam_role_policy_attachment" "stepfunctions_glue_job_management" {
  role       = aws_iam_role.stepfunctions_file_processing.name
  policy_arn = "arn:aws:iam::131912110087:policy/service-role/GlueJobRunManagementFullAccessPolicy-275a90ec-b616-4d80-9117-db78d875ef4f"
}

resource "aws_iam_role_policy_attachment" "stepfunctions_xray" {
  role       = aws_iam_role.stepfunctions_file_processing.name
  policy_arn = "arn:aws:iam::131912110087:policy/service-role/XRayAccessPolicy-eabafdb2-519e-48eb-81a1-d56784e46b0a"
}
