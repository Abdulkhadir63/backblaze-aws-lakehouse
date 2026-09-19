resource "aws_iam_role" "glue" {
  name = "Glue-S3-Access-Role"
  path = "/"

  description = "Allows Glue to call AWS services on your behalf. "

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "glue.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })

  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "S3-access-glue"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "BackblazeLakehouseBucketAccess"
        Effect = "Allow"

        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]

        Resource = aws_s3_bucket.lakehouse.arn
      },
      {
        Sid    = "BackblazeLakehouseObjectAccess"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]

        Resource = [
          "${aws_s3_bucket.lakehouse.arn}/schemas/*",
          "${aws_s3_bucket.lakehouse.arn}/raw/*",
          "${aws_s3_bucket.lakehouse.arn}/iceberg/*",
          "${aws_s3_bucket.lakehouse.arn}/quarantine/*",
          "${aws_s3_bucket.lakehouse.arn}/glue_scripts/*"
        ]
      }
    ]
  })
}
