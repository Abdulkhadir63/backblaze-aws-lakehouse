resource "aws_s3_bucket_notification" "raw_events" {
  bucket = aws_s3_bucket.lakehouse.id

  queue {
    id            = "backblaze-raw-object-created"
    queue_arn     = "arn:aws:sqs:ap-south-1:131912110087:backblaze-dev-s3-events"
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/drivestats/"
    filter_suffix = ".csv"
  }
}
