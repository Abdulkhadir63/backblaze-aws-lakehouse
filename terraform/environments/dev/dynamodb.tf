resource "aws_dynamodb_table" "pipeline_control" {
  name         = "backblaze-dev-pipeline-control"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "control_id"
  table_class  = "STANDARD"

  deletion_protection_enabled = false

  attribute {
    name = "control_id"
    type = "S"
  }

  attribute {
    name = "gsi_pk"
    type = "S"
  }

  attribute {
    name = "gsi_sk"
    type = "S"
  }

  global_secondary_index {
    name            = "backblaze-processing-queue-index"
    hash_key        = "gsi_pk"
    range_key       = "gsi_sk"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = false
  }
}
