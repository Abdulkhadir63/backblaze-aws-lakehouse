resource "aws_iam_role_policy" "github_actions_glue" {
  name = "GitHubActionsGlueDeployment"
  role = aws_iam_role.github_actions_artifact.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "ManageGlueArtifacts"
        Effect = "Allow"

        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload",
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]

        Resource = "${aws_s3_bucket.deployment_artifacts.arn}/glue/*"
      },
      {
        Sid    = "DeployGlueJobs"
        Effect = "Allow"

        Action = [
          "glue:GetJob",
          "glue:UpdateJob"
        ]

        Resource = [
          "arn:aws:glue:ap-south-1:131912110087:job/bronze_ingestion",
          "arn:aws:glue:ap-south-1:131912110087:job/silver_cleaned",
          "arn:aws:glue:ap-south-1:131912110087:job/data_quality_check",
          "arn:aws:glue:ap-south-1:131912110087:job/gold_layer",
          "arn:aws:glue:ap-south-1:131912110087:job/bronze_layer",
          "arn:aws:glue:ap-south-1:131912110087:job/silver_layer",
          "arn:aws:glue:ap-south-1:131912110087:job/data_quality_layer",
          "arn:aws:glue:ap-south-1:131912110087:job/gold_analytics_layer"
        ]
      },
      {
        Sid    = "PassGlueExecutionRole"
        Effect = "Allow"

        Action = [
          "iam:PassRole"
        ]

        Resource = "arn:aws:iam::131912110087:role/Glue-S3-Access-Role"

        Condition = {
          StringEquals = {
            "iam:PassedToService" = "glue.amazonaws.com"
          }
        }
      }
    ]
  })
}
