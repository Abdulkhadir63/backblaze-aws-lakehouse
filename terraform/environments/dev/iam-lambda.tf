resource "aws_iam_role" "lambda_event_handler" {
  name = "backblaze-dev-s3-event-handler-role-tmlsc7z7"
  path = "/service-role/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "lambda.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })

  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "lambda_sqs_full_access" {
  role       = aws_iam_role.lambda_event_handler.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSQSFullAccess"
}

resource "aws_iam_role_policy_attachment" "lambda_dynamodb_full_access" {
  role       = aws_iam_role.lambda_event_handler.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_event_handler.name
  policy_arn = "arn:aws:iam::131912110087:policy/service-role/AWSLambdaBasicExecutionRole-21b37a66-4418-4fa8-92f1-03b1f9fa6a27"
}

resource "aws_iam_role_policy" "lambda_start_step_function" {
  name = "StartBackblazeStepFunction"
  role = aws_iam_role.lambda_event_handler.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "StartBackblazeProcessingStateMachine"
        Effect = "Allow"

        Action = "states:StartExecution"

        Resource = "arn:aws:states:ap-south-1:131912110087:stateMachine:backblaze-dev-file-processing"
      }
    ]
  })
}
