resource "aws_iam_role" "github_oidc_diagnostic" {
  name = "backblaze-dev-github-oidc-diagnostic"

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
        }

        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:Abdulkhadir63@307916668/backblaze-aws-lakehouse@1376982195:*"
        }
      }
    }]
  })
}