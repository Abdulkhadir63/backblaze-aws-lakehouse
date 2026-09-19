# Backblaze AWS Lakehouse

Production-oriented AWS lakehouse for Backblaze Drive Stats.

## Processing Modes

### Full Load / Backfill
Historical release-level processing using --release_id.

This path is manually triggered and is not started by Terraform deployments.

### Incremental Load
Event-driven processing of one newly arrived S3 object:

S3 ? SQS ? Lambda ? DynamoDB ? Step Functions ? Glue ? SNS

## AWS Region

ap-south-1

## Data Storage

S3 + Iceberg

## Infrastructure

Terraform

## CI/CD

GitHub Actions + AWS OIDC
