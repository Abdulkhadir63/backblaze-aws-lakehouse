resource "aws_sns_topic_policy" "pipeline_notifications" {
  arn = aws_sns_topic.pipeline_notifications.arn

  policy = jsonencode({
    Version = "2008-10-17"
    Id      = "__default_policy_ID"

    Statement = [
      {
        Sid    = "__default_statement_ID"
        Effect = "Allow"

        Principal = {
          AWS = "*"
        }

        Action = [
          "SNS:Publish",
          "SNS:RemovePermission",
          "SNS:SetTopicAttributes",
          "SNS:DeleteTopic",
          "SNS:ListSubscriptionsByTopic",
          "SNS:GetTopicAttributes",
          "SNS:AddPermission",
          "SNS:Subscribe"
        ]

        Resource = aws_sns_topic.pipeline_notifications.arn

        Condition = {
          StringEquals = {
            "AWS:SourceAccount" = "131912110087"
          }
        }
      }
    ]
  })
}
