resource "aws_sqs_queue_policy" "main" {
  queue_url = aws_sqs_queue.main.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "AllowS3BucketToSendMessages"
        Effect = "Allow"

        Principal = {
          Service = "s3.amazonaws.com"
        }

        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.main.arn

        Condition = {
          StringEquals = {
            "aws:SourceAccount" = "131912110087"
          }

          ArnEquals = {
            "aws:SourceArn" = "arn:aws:s3:::backblaze-de-lakehouse-project"
          }
        }
      }
    ]
  })
}
