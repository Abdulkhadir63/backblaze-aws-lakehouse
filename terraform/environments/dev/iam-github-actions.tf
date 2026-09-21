resource "aws_iam_role" "github_actions_artifact" {
  name = "backblaze-dev-github-actions-artifact-role"
  path = "/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [{
      Effect = "Allow"

      Principal = {
        Federated = aws_iam_openid_connect_provider.github_actions.arn
      }

      Action = "sts:AssumeRoleWithWebIdentity"

      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"

          "token.actions.githubusercontent.com:sub" = "repo:Abdulkhadir63@307916686/backblaze-aws-lakehouse@1376982195:ref:refs/heads/main"
        }
      }
    }]
  })

  max_session_duration = 3600
}

resource "aws_iam_role_policy" "github_actions_artifact" {
  name = "GitHubActionsArtifactUpload"
  role = aws_iam_role.github_actions_artifact.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "GetArtifactBucketLocation"
        Effect = "Allow"

        Action = [
          "s3:GetBucketLocation"
        ]

        Resource = aws_s3_bucket.deployment_artifacts.arn
      },
      {
        Sid    = "ManageLambdaArtifacts"
        Effect = "Allow"

        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload",
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]

        Resource = "${aws_s3_bucket.deployment_artifacts.arn}/lambda/backblaze-dev-s3-event-handler/*"
      },
      {
        Sid    = "DeployLambdaCode"
        Effect = "Allow"

        Action = [
          "lambda:GetFunction",
          "lambda:GetFunctionConfiguration",
          "lambda:UpdateFunctionCode"
        ]

        Resource = "arn:aws:lambda:ap-south-1:131912110087:function:backblaze-dev-s3-event-handler"
      }
    ]
  })
}