# Deployment Guide — Backblaze AWS Lakehouse

## 1. Purpose

This document is the deployment runbook for the Backblaze AWS Lakehouse project.

The deployment model separates infrastructure ownership from application-code ownership:

```text
Terraform
  └── AWS infrastructure and job/function configuration

GitHub Actions + OIDC
  ├── Lambda application releases
  └── Glue script releases
```

The deployment target is AWS `ap-south-1`.

---

## 2. Deployment Architecture

```text
GitHub main
    |
    +-----------------------------+
    |                             |
    v                             v
Terraform                    GitHub Actions
    |                             |
    |                             +--> Lambda ZIP
    |                             |      |
    |                             |      v
    |                             |   S3 artifact bucket
    |                             |      |
    |                             |      v
    |                             |   Lambda published version
    |                             |
    |                             +--> Glue Python scripts
    |                                    |
    |                                    v
    |                                 S3 artifact bucket
    |                                    |
    |                                    v
    |                                 Glue jobs
    |
    +--> S3 / SQS / Lambda mapping
    +--> DynamoDB
    +--> Step Functions
    +--> IAM / OIDC
    +--> Glue job infrastructure
    +--> SNS
```

Runtime data flow is separate from deployment:

```text
S3 RAW
  -> SQS
  -> Lambda
  -> DynamoDB control plane
  -> Step Functions
  -> Glue Bronze
  -> Glue Silver
  -> Glue DQ
  -> Glue Gold
```

Deployment success does not by itself prove that the runtime pipeline has completed a clean end-to-end data run.

---

## 3. Repository Layout

Important deployment paths:

```text
.github/workflows/
  deploy-lambda.yml
  deploy-glue.yml

lambda/s3_event_handler/
  lambda_function.py

glue_scripts/scripts/
  full-load/
    bronze/bronze_ingestion.py
    silver/silver_cleaned.py
    data_quality_check/data_quality_check.py
    gold/gold_layer.py
  incremental-load/
    bronze/bronze_layer.py
    silver/silver_layer.py
    data_quality_check/data_quality_layer.py
    gold/gold_analytics_layer.py

stepfunctions/
  backblaze_file_processing.asl.json

terraform/environments/dev/
  *.tf
```

Terraform state is remote. The development environment is:

```text
terraform/environments/dev
```

Do not run Terraform from a random directory. Run it from the environment directory above.

---

## 4. AWS Resources

Primary lakehouse bucket:

```text
backblaze-de-lakehouse-project
```

Deployment artifact bucket:

```text
backblaze-dev-deployment-artifacts-131912110087
```

Terraform state bucket:

```text
backblaze-de-terraform-state-131912110087
```

Event-driven resources:

```text
SQS:             backblaze-dev-s3-events
SQS DLQ:         backblaze-dev-s3-events-dlq
Lambda:          backblaze-dev-s3-event-handler
DynamoDB:        backblaze-dev-pipeline-control
Step Functions:  backblaze-dev-file-processing
SNS:             backblaze-dev-pipeline-notifications
```

Glue jobs:

```text
bronze_ingestion
silver_cleaned
data_quality_check
gold_layer

bronze_layer
silver_layer
data_quality_layer
gold_analytics_layer
```

---

## 5. Prerequisites

Required local tools:

```text
Git
AWS CLI
Terraform
Python
```

AWS CLI must be authenticated to the deployment AWS account.

Check identity:

```powershell
aws sts get-caller-identity
```

Terraform uses the committed provider lock file.

---

# 6. Terraform Deployment

## 6.1 Enter the environment

```powershell
cd C:\Users\ASUS\Desktop\backblaze-aws-lakehouse\terraform\environments\dev
```

## 6.2 Initialize Terraform

```powershell
terraform init
```

## 6.3 Validate

```powershell
terraform validate
```

Warnings about deprecated Terraform provider arguments such as DynamoDB `hash_key` / `range_key` are currently non-blocking. Do not mix that modernization work into a deployment release unless it is intentionally being changed.

## 6.4 Review the plan

```powershell
terraform plan
```

Expected steady state:

```text
No changes. Your infrastructure matches the configuration.
```

Do not apply a plan showing unexpected resource changes.

## 6.5 Apply infrastructure changes

Only after reviewing the plan:

```powershell
terraform apply
```

Approve only the changes expected for the deployment.

## 6.6 Post-apply verification

```powershell
terraform plan
```

The final result must return to:

```text
No changes. Your infrastructure matches the configuration.
```

---

# 7. GitHub OIDC Deployment

GitHub Actions uses AWS IAM OIDC rather than long-lived AWS access keys.

The production deployment role is:

```text
backblaze-dev-github-actions-artifact-role
```

The trust policy is restricted to the repository/main-branch subject configured in:

```text
terraform/environments/dev/iam-github-actions.tf
```

Do not replace this with static AWS credentials.

The Lambda and Glue workflows both use:

```yaml
permissions:
  contents: read
  id-token: write
```

and:

```yaml
uses: aws-actions/configure-aws-credentials@v6.3.0
```

---

# 8. Lambda Deployment

Workflow:

```text
.github/workflows/deploy-lambda.yml
```

Trigger:

```text
workflow_dispatch
```

This means Lambda deployment is intentionally manual from GitHub Actions.

## 8.1 Deployment flow

```text
Checkout
  ->
Python syntax validation
  ->
GitHub OIDC
  ->
AWS identity verification
  ->
Build ZIP
  ->
SHA256 calculation
  ->
Upload immutable artifact to S3
  ->
Read S3 VersionId
  ->
Update Lambda from that S3 object version
  ->
Publish Lambda version
  ->
Verify CodeSha256
  ->
Verify Lambda configuration
```

## 8.2 Lambda artifact convention

Artifacts are stored under:

```text
lambda/backblaze-dev-s3-event-handler/<GITHUB_SHA>.zip
```

The deployment bucket is versioned, so each artifact has an S3 `VersionId`.

## 8.3 Lambda verification

```powershell
aws lambda list-versions-by-function `
  --function-name backblaze-dev-s3-event-handler `
  --query 'Versions[].{Version:Version,CodeSha256:CodeSha256,LastModified:LastModified}' `
  --output table
```

The active published deployment should appear as a numbered Lambda version, not only `$LATEST`.

## 8.4 Smoke test

Use an SQS-shaped event when testing the handler because the Lambda is invoked through an SQS event source mapping.

A safe smoke test should deliberately use an invalid bucket and confirm that the deployed Lambda rejects it during validation.

This proves the published Lambda version is executing the newly deployed code without triggering the real pipeline.

---

# 9. Glue Deployment

Workflow:

```text
.github/workflows/deploy-glue.yml
```

Trigger:

```text
workflow_dispatch
```

## 9.1 Deployment flow

```text
Checkout
  ->
Discover exactly 8 Glue Python scripts
  ->
Python syntax validation
  ->
GitHub OIDC
  ->
AWS identity verification
  ->
Upload each script to immutable S3 release path
  ->
Read S3 VersionId
  ->
Read current Glue job definition
  ->
Preserve existing job configuration
  ->
Change only Command.ScriptLocation
  ->
Update Glue job
  ->
Verify ScriptLocation
  ->
Write deployment manifest
  ->
Final verification of all 8 jobs
```

## 9.2 Glue artifact convention

The deployment path is:

```text
glue/<GITHUB_SHA>/<GITHUB_RUN_ID>/<GITHUB_RUN_ATTEMPT>/<relative-script-path>
```

Example structure:

```text
glue/
  <commit-sha>/
    <run-id>/
      <attempt>/
        full-load/...
        incremental-load/...
        glue-deployment-manifest.json
```

This keeps deployment attempts immutable and prevents a rerun from silently overwriting the prior release.

## 9.3 Critical Glue deployment behavior

AWS Glue `UpdateJob` replaces the job definition. Therefore the workflow does not send only a new `ScriptLocation`.

The workflow first reads the current Glue job, preserves its existing settings, and changes only:

```text
Command.ScriptLocation
```

This preserves settings such as:

```text
GlueVersion
WorkerType
NumberOfWorkers
Timeout
MaxRetries
ExecutionClass
DefaultArguments
ExecutionProperty
```

## 9.4 Glue deployment IAM

The GitHub deployment role requires:

```text
s3:PutObject
s3:AbortMultipartUpload
s3:GetObject
s3:GetObjectVersion
```

for:

```text
deployment-artifacts-bucket/glue/*
```

and:

```text
glue:GetJob
glue:UpdateJob
```

for the eight Glue jobs.

Because the Glue job execution role is passed during job updates, the GitHub role also requires:

```text
iam:PassRole
```

restricted to:

```text
Glue-S3-Access-Role
```

with:

```text
iam:PassedToService = glue.amazonaws.com
```

Do not remove this permission or the Glue deployment will fail at `UpdateJob`.

---

# 10. Glue Infrastructure Ownership

Terraform owns the eight Glue job resources.

Source:

```text
terraform/environments/dev/glue.tf
```

The eight live jobs were adopted into Terraform before CI/CD was enabled.

Terraform intentionally ignores:

```hcl
command[0].script_location
```

for these jobs.

Reason:

```text
Terraform
  -> job infrastructure

GitHub Actions
  -> deployed application script
```

This prevents Terraform from overwriting the application release every time infrastructure is planned or applied.

---

# 11. Step Functions Deployment

Step Functions is Terraform-managed.

Source definition:

```text
stepfunctions/backblaze_file_processing.asl.json
```

Terraform resource:

```text
terraform/environments/dev/stepfunctions.tf
```

There is no separate Step Functions application deployment workflow.

## 11.1 Verification

```powershell
aws stepfunctions describe-state-machine `
  --state-machine-arn arn:aws:states:ap-south-1:131912110087:stateMachine:backblaze-dev-file-processing `
  --query '{Name:name,Status:status,Type:type,RoleArn:roleArn}' `
  --output table
```

Expected:

```text
Status = ACTIVE
Type   = STANDARD
```

The deployment verification should also compare a canonicalized SHA256 of:

```text
local ASL
```

against:

```text
AWS deployed definition
```

A matching SHA256 proves the repository definition and deployed state machine are identical.

---

# 12. Event-Driven Wiring Verification

## 12.1 S3 -> SQS

The S3 event notification must target:

```text
backblaze-dev-s3-events
```

for:

```text
s3:ObjectCreated:*
```

with:

```text
Prefix = raw/drivestats/
Suffix = .csv
```

Verification:

```powershell
aws s3api get-bucket-notification-configuration `
  --bucket backblaze-de-lakehouse-project `
  --output json
```

## 12.2 SQS -> Lambda

The Lambda event-source mapping must be enabled.

Verification:

```powershell
aws lambda list-event-source-mappings `
  --function-name backblaze-dev-s3-event-handler `
  --query 'EventSourceMappings[].{UUID:UUID,State:State,BatchSize:BatchSize,EventSourceArn:EventSourceArn}' `
  --output table
```

Current intended configuration:

```text
State     = Enabled
BatchSize = 1
```

---

# 13. Final Deployment Audit

Run these checks after a deployment release:

```powershell
cd C:\Users\ASUS\Desktop\backblaze-aws-lakehouse
git status --short
git log -1 --oneline
```

Then:

```powershell
cd .\terraform\environments\dev
terraform plan
```

Expected:

```text
No changes. Your infrastructure matches the configuration.
```

Verify Lambda:

```powershell
aws lambda get-function `
  --function-name backblaze-dev-s3-event-handler `
  --qualifier <PUBLISHED_VERSION> `
  --query '{FunctionName:Configuration.FunctionName,Version:Configuration.Version,Runtime:Configuration.Runtime,Handler:Configuration.Handler,CodeSha256:Configuration.CodeSha256}' `
  --output table
```

Verify Glue:

```powershell
aws glue get-job `
  --job-name <JOB_NAME> `
  --query 'Job.[Name,GlueVersion,WorkerType,NumberOfWorkers,Timeout,MaxRetries,ExecutionClass,Command.ScriptLocation]' `
  --output json
```

Verify Step Functions:

```powershell
aws stepfunctions describe-state-machine `
  --state-machine-arn arn:aws:states:ap-south-1:131912110087:stateMachine:backblaze-dev-file-processing `
  --query '{Name:name,Status:status,Type:type}' `
  --output table
```

Verify Git:

```powershell
git status --short
```

A clean deployment should leave no uncommitted changes.

---

# 14. Rollback

## Lambda

Lambda deployments publish numbered versions.

Use the previously verified Lambda version as the rollback target.

The current CI workflow does not automatically manage an alias. Operational rollback therefore means explicitly restoring a known-good published version rather than relying on `$LATEST`.

Keep the corresponding immutable S3 artifact available.

## Glue

Glue deployments use immutable S3 artifact paths.

A rollback should point the Glue jobs back to a known-good artifact release.

The current workflow does not expose a dedicated rollback parameter. The practical repository-based rollback process is:

```text
Identify known-good Git commit
  ->
Restore/revert repository to that code
  ->
Run Deploy Glue manually
  ->
Verify all 8 ScriptLocations
```

For emergency rollback, use the previously recorded deployment manifest and immutable S3 paths.

## Terraform

Never use:

```text
terraform -lock=false
```

for normal recovery.

Do not use `terraform apply` to force application-code rollback when GitHub Actions owns Lambda/Glue code release paths.

---

# 15. Deployment Evidence

The deployment phase was verified on 2026-09-21.

Verified evidence included:

```text
Terraform plan                 No changes
Lambda published version       2
Glue CI/CD                     Successful
All 8 Glue ScriptLocations     Versioned deployment artifacts
Step Functions                 ACTIVE / STANDARD
S3 -> SQS                      Configured
SQS -> Lambda                  Enabled
Git working tree               Clean
```

The latest repository deployment commit at the time of final audit was:

```text
f988cf4
Allow GitHub Actions to pass Glue execution role
```

The verified Lambda deployment was:

```text
Lambda version: 2
```

The verified Glue deployment used an immutable artifact release path under the `glue/` prefix.

---

# 16. Known Runtime Reliability Item

Deployment correctness and runtime correctness are separate concerns.

A previous test execution left the DynamoDB pipeline control record in:

```text
PROCESSING
```

after a Step Functions execution was externally aborted while a Glue stage was running.

The normal state-machine failure path was not reached, so the control-plane cleanup did not run.

This is a **runtime recovery/reconciliation problem**, not a deployment failure.

Do not manually change the DynamoDB state as part of a deployment verification.

The recovery design should be addressed separately and tested independently.

---

# 17. Important Operational Rules

Do not:

```text
Use long-lived AWS access keys in GitHub
Run historical backfills from Terraform
Let Terraform overwrite CI/CD-managed Lambda code
Let Terraform overwrite CI/CD-managed Glue ScriptLocations
Use git add . blindly during deployment changes
Apply a Terraform plan containing unexpected destructive changes
Start real Glue jobs merely to prove code deployment
Claim end-to-end pipeline success from infrastructure deployment alone
```

Do:

```text
Review terraform plan
Keep application artifacts immutable
Use Git commit SHA in deployment paths
Verify deployed checksums / ScriptLocations
Keep Terraform and application-code ownership separate
Record deployment evidence
Keep rollback artifacts
```

---

# 18. Deployment Completion Criteria

The deployment phase is considered complete when all of the following are true:

```text
[ ] Git working tree clean
[ ] Terraform validate succeeds
[ ] Terraform plan reports no unexpected changes
[ ] GitHub OIDC works
[ ] Lambda CI/CD succeeds
[ ] Lambda published version verified
[ ] Glue source passes syntax validation
[ ] Glue CI/CD succeeds
[ ] All 8 Glue ScriptLocations verified
[ ] Step Functions deployed definition matches repository
[ ] S3 -> SQS configuration verified
[ ] SQS -> Lambda mapping enabled
[ ] Deployment evidence recorded
```

A clean deployment does not automatically mean a clean end-to-end runtime execution. Treat runtime testing and recovery testing as a separate milestone.
