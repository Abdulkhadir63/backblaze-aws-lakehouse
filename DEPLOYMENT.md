#  Deployment

This project is deployed using two separate mechanisms:

- **Terraform** manages AWS infrastructure and service configuration.
- **GitHub Actions + AWS OIDC** manages application-code releases for Lambda and AWS Glue.

The important design decision is that infrastructure and application code are not owned by the same deployment mechanism.

```text
Git repository
      |
      +------------------------------+
      |                              |
      v                              v
Terraform                       GitHub Actions
      |                              |
      |                              +---- Lambda ZIP
      |                              |       |
      |                              |       v
      |                              |    S3 artifact
      |                              |       |
      |                              |       v
      |                              |    Lambda version
      |                              |
      |                              +---- Glue scripts
      |                                      |
      |                                      v
      |                                   S3 artifacts
      |                                      |
      |                                      v
      |                                   Glue jobs
      |
      +---- S3
      +---- SQS
      +---- DynamoDB
      +---- Lambda configuration
      +---- Step Functions
      +---- IAM
      +---- GitHub OIDC
      +---- Glue job infrastructure
      +---- SNS
```

The deployment target is AWS `ap-south-1`.

---

## 🧑‍💻 Clone the Repository

A new developer starts from the Git repository instead of manually rebuilding the AWS environment.

```powershell
git clone https://github.com/Abdulkhadir63/backblaze-aws-lakehouse.git

cd backblaze-aws-lakehouse
```

After cloning, the repository contains the Terraform configuration, Lambda source code, Glue scripts, Step Functions definition, GitHub Actions workflows, and deployment configuration.

```text
backblaze-aws-lakehouse/
│
├── .github/
│   └── workflows/
│       ├── deploy-lambda.yml
│       └── deploy-glue.yml
│
├── lambda/
│   └── s3_event_handler/
│       └── lambda_function.py
│
├── glue_scripts/
│   └── scripts/
│       ├── full-load/
│       │   ├── bronze/
│       │   ├── silver/
│       │   ├── data_quality_check/
│       │   └── gold/
│       │
│       └── incremental-load/
│           ├── bronze/
│           ├── silver/
│           ├── data_quality_check/
│           └── gold/
│
├── stepfunctions/
│   └── backblaze_file_processing.asl.json
│
└── terraform/
    └── environments/
        └── dev/
```

The Terraform working directory is:

```powershell
cd .\terraform\environments\dev
```

Do not run Terraform from the repository root unless the project is explicitly configured to do so.

---

# 🧰 Deployment Prerequisites

The deployment machine requires:

```text
Git
AWS CLI
Terraform
Python
```

Verify Git:

```powershell
git --version
```

Verify AWS CLI:

```powershell
aws --version
```

Verify Terraform:

```powershell
terraform version
```

Verify Python:

```powershell
python --version
```

The AWS CLI must be authenticated to the AWS account that owns the environment.

Verify the active AWS identity:

```powershell
aws sts get-caller-identity
```

The returned account must be the intended deployment account.

---

# ☁️ AWS Environment

The current deployment uses:

```text
AWS Account : 131912110087
AWS Region  : ap-south-1
```

The primary lakehouse bucket is:

```text
backblaze-de-lakehouse-project
```

The deployment artifact bucket is:

```text
backblaze-dev-deployment-artifacts-131912110087
```

The Terraform remote-state bucket is:

```text
backblaze-de-terraform-state-131912110087
```

Runtime resources:

```text
SQS
    backblaze-dev-s3-events

SQS DLQ
    backblaze-dev-s3-events-dlq

Lambda
    backblaze-dev-s3-event-handler

DynamoDB
    backblaze-dev-pipeline-control

Step Functions
    backblaze-dev-file-processing

SNS
    backblaze-dev-pipeline-notifications
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

# 🏗️ First-Time Infrastructure Deployment

The first deployment is different from normal application releases.

Terraform uses an S3 remote backend:

```text
S3
└── backblaze-de-terraform-state-131912110087
```

The backend must exist before Terraform can initialize against it.

This is an intentional bootstrap boundary.

After the backend exists, normal infrastructure deployment is handled by Terraform.

The project uses Terraform's native S3 state locking mechanism and should not be operated with:

```powershell
terraform -lock=false
```

---

# 🔧 Terraform Initialization

Move into the Terraform environment:

```powershell
cd C:\Users\ASUS\Desktop\backblaze-aws-lakehouse\terraform\environments\dev
```

Initialize Terraform:

```powershell
terraform init
```

Terraform downloads the configured AWS provider and connects the working directory to the remote state backend.

The provider version is locked by the repository lock file.

Do not delete the lock file simply to make initialization succeed with a different provider version.

---

# ✅ Terraform Validation

Validate the configuration before planning:

```powershell
terraform validate
```

Expected result:

```text
Success! The configuration is valid.
```

Validation checks the Terraform configuration itself.

It does not prove that AWS resources match the configuration.

---

# 🔍 Terraform Plan

Run:

```powershell
terraform plan
```

The plan is the mandatory review point before infrastructure changes.

For the current deployed environment, the verified steady state is:

```text
No changes. Your infrastructure matches the configuration.
```

Do not apply a plan containing unexpected:

```text
destroy
replace
modify
```

operations.

Review the resource responsible for the change before applying it.

---

# 🚀 Terraform Apply

Only apply after reviewing the plan:

```powershell
terraform apply
```

Terraform manages infrastructure such as:

```text
S3
SQS
Lambda configuration
Lambda IAM
DynamoDB
Step Functions
Step Functions IAM
SNS
Glue job definitions
Glue IAM
GitHub OIDC
GitHub Actions deployment IAM
S3 event notifications
Lambda event-source mapping
```

Terraform is responsible for creating or updating the infrastructure definition.

Terraform is not the mechanism used to release Lambda application code or Glue application scripts.

---

# 🔁 Post-Apply Drift Check

After applying Terraform:

```powershell
terraform plan
```

The desired result is:

```text
No changes. Your infrastructure matches the configuration.
```

This second plan confirms that the applied infrastructure matches the repository configuration.

---

# 🔐 GitHub Actions Authentication

The project does not use long-lived AWS access keys inside GitHub Actions.

GitHub Actions authenticates to AWS using:

```text
GitHub OIDC
        |
        v
AWS IAM Role
        |
        v
Temporary AWS credentials
```

The deployment role is:

```text
backblaze-dev-github-actions-artifact-role
```

The workflows request:

```yaml
permissions:
  contents: read
  id-token: write
```

and authenticate using:

```yaml
aws-actions/configure-aws-credentials@v6.3.0
```

The IAM trust policy is restricted to the expected GitHub repository and branch.

This means another repository cannot automatically assume the role.

---

# ⚠️ Important When Another Developer Forks or Copies the Repository

The repository is deployable, but the AWS account and GitHub OIDC trust relationship are environment-specific.

The current IAM trust configuration references the actual GitHub repository and branch.

Therefore a completely different repository cannot simply run:

```powershell
terraform apply
```

without updating environment-specific values.

A new deployment into the same environment can clone the repository and use the existing configuration.

A deployment into another AWS account requires changing at least:

```text
AWS account-specific resource names
Terraform backend
GitHub OIDC trust subject
IAM resource references
Deployment artifact bucket
Terraform state bucket
```

The infrastructure code is reusable, but the environment identity is intentionally explicit.

---

# ⚡ Lambda Application Deployment

Lambda infrastructure is Terraform-managed.

Lambda application code is GitHub Actions-managed.

The workflow is:

```text
Developer changes lambda_function.py
              |
              v
Git commit
              |
              v
GitHub Actions
              |
              v
Python syntax validation
              |
              v
AWS OIDC authentication
              |
              v
Build ZIP
              |
              v
Calculate SHA256
              |
              v
Upload immutable ZIP to S3
              |
              v
Read S3 VersionId
              |
              v
Update Lambda
              |
              v
Publish Lambda version
              |
              v
Verify CodeSha256
```

Workflow:

```text
.github/workflows/deploy-lambda.yml
```

The workflow is manually triggered with:

```text
workflow_dispatch
```

This prevents every repository push from automatically deploying application code.

---

# 📦 Lambda Artifact Storage

Each Lambda release is stored under:

```text
lambda/backblaze-dev-s3-event-handler/<GITHUB_SHA>.zip
```

The commit SHA becomes part of the artifact path.

The artifact is immutable.

The deployment bucket also maintains an S3 `VersionId`.

This provides two release identifiers:

```text
Git commit SHA
S3 VersionId
```

The deployed Lambda additionally receives a numbered published Lambda version.

---

# 🔎 Lambda Deployment Verification

List deployed versions:

```powershell
aws lambda list-versions-by-function `
  --function-name backblaze-dev-s3-event-handler `
  --query 'Versions[].{Version:Version,CodeSha256:CodeSha256,LastModified:LastModified}' `
  --output table
```

Verify a specific published version:

```powershell
aws lambda get-function `
  --function-name backblaze-dev-s3-event-handler `
  --qualifier <PUBLISHED_VERSION> `
  --query '{FunctionName:Configuration.FunctionName,Version:Configuration.Version,Runtime:Configuration.Runtime,Handler:Configuration.Handler,CodeSha256:Configuration.CodeSha256}' `
  --output table
```

The deployment must correspond to a numbered Lambda version.

Do not treat `$LATEST` alone as release evidence.

---

# 🧪 Lambda Deployment Smoke Test

The Lambda is invoked through:

```text
SQS
  |
  v
Lambda
```

Therefore the smoke test uses an SQS-shaped event.

The deployment test uses an intentionally invalid S3 bucket.

The purpose is not to process real data.

The purpose is to prove:

```text
SQS event format
       |
       v
published Lambda version
       |
       v
new application code
       |
       v
validation logic executes
```

The verified deployment rejected the invalid bucket with:

```text
Unexpected bucket: deployment-smoke-test-invalid-bucket
```

This proved that the newly deployed Lambda code was executing.

---

# 🧱 Glue Application Deployment

There are eight Glue application scripts:

```text
full-load/
    bronze_ingestion.py
    silver_cleaned.py
    data_quality_check.py
    gold_layer.py

incremental-load/
    bronze_layer.py
    silver_layer.py
    data_quality_layer.py
    gold_analytics_layer.py
```

Glue infrastructure is managed by Terraform.

Glue application scripts are deployed by GitHub Actions.

Workflow:

```text
.github/workflows/deploy-glue.yml
```

The deployment is manually triggered with:

```text
workflow_dispatch
```

---

# 🔄 Glue CI/CD Release Flow

The Glue deployment performs:

```text
Checkout repository
        |
        v
Verify exactly 8 expected scripts
        |
        v
Python syntax validation
        |
        v
GitHub OIDC authentication
        |
        v
AWS identity verification
        |
        v
Upload scripts to immutable S3 paths
        |
        v
Capture S3 VersionId
        |
        v
Read existing Glue job definitions
        |
        v
Preserve existing job configuration
        |
        v
Change only ScriptLocation
        |
        v
Update Glue jobs
        |
        v
Verify ScriptLocation
        |
        v
Write deployment manifest
        |
        v
Verify all 8 jobs
```

---

# 📁 Glue Artifact Convention

Scripts are deployed under:

```text
glue/<GITHUB_SHA>/<GITHUB_RUN_ID>/<GITHUB_RUN_ATTEMPT>/<relative-script-path>
```

Example:

```text
glue/
└── daf117ce548c7c3d6d6b30515ee51f4b86acd25b/
    └── 35584853075/
        └── 2/
            ├── full-load/
            │   ├── bronze/
            │   ├── silver/
            │   ├── data_quality_check/
            │   └── gold/
            │
            └── incremental-load/
                ├── bronze/
                ├── silver/
                ├── data_quality_check/
                └── gold/
```

This makes every deployment traceable to:

```text
Git commit
+
GitHub workflow run
+
workflow attempt
+
script
```

A rerun produces a separate artifact path rather than silently overwriting an existing release.

---

# ⚠️ Glue UpdateJob Ownership

AWS Glue `UpdateJob` replaces the job definition.

Therefore the CI/CD workflow does not send only:

```text
Command.ScriptLocation
```

Instead it:

```text
Reads current Glue job
        |
        v
Preserves existing settings
        |
        v
Changes ScriptLocation
        |
        v
Updates Glue job
```

The preserved configuration includes:

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

This prevents an application-code deployment from accidentally resetting the compute configuration.

---

# 🔐 Glue Deployment IAM

The GitHub deployment role requires S3 permissions for the deployment artifact path:

```text
s3:PutObject
s3:AbortMultipartUpload
s3:GetObject
s3:GetObjectVersion
```

It also requires:

```text
glue:GetJob
glue:UpdateJob
```

for the eight Glue jobs.

Because the Glue job definition contains an execution role, the GitHub Actions role also requires:

```text
iam:PassRole
```

restricted to:

```text
Glue-S3-Access-Role
```

and restricted with:

```text
iam:PassedToService = glue.amazonaws.com
```

This permission was required to complete the real Glue CI/CD deployment.

---

# 🏗️ Terraform vs CI/CD Ownership

This project deliberately separates infrastructure ownership from application-code ownership.

```text
+------------------------+--------------------------+
| Resource               | Owner                    |
+------------------------+--------------------------+
| S3 buckets             | Terraform                |
| SQS queues             | Terraform                |
| DynamoDB               | Terraform                |
| Lambda configuration   | Terraform                |
| Lambda application code| GitHub Actions           |
| Glue job infrastructure| Terraform               |
| Glue Python scripts    | GitHub Actions           |
| Step Functions         | Terraform                |
| IAM                    | Terraform                |
| GitHub OIDC            | Terraform                |
| SNS                    | Terraform                |
+------------------------+--------------------------+
```

Terraform intentionally ignores the Lambda deployment package properties controlled by CI/CD.

Terraform also ignores:

```hcl
command[0].script_location
```

for the Glue jobs.

This prevents the following failure:

```text
GitHub Actions deploys new application code
            |
            v
Terraform runs later
            |
            v
Terraform overwrites application release
```

The two deployment systems therefore have separate responsibilities.

---

# 🔀 Step Functions Deployment

Step Functions is Terraform-managed.

The repository definition is:

```text
stepfunctions/backblaze_file_processing.asl.json
```

The Terraform resource is defined in:

```text
terraform/environments/dev/stepfunctions.tf
```

There is no separate GitHub Actions workflow for Step Functions.

Terraform deploys the state machine definition as infrastructure.

Verify the deployed state machine:

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

The deployed definition was also compared against the repository ASL after canonicalization.

A matching SHA256 confirms that the repository state-machine definition and deployed AWS definition are identical.

---

# 🔗 Event-Driven Infrastructure Verification

After infrastructure deployment, verify the actual event chain.

```text
S3
 |
 v
SQS
 |
 v
Lambda
 |
 v
DynamoDB
 |
 v
Step Functions
 |
 v
Glue
```

---

# 📤 S3 → SQS

The S3 notification targets:

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

Verify:

```powershell
aws s3api get-bucket-notification-configuration `
  --bucket backblaze-de-lakehouse-project `
  --output json
```

---

# 📨 SQS → Lambda

The Lambda event-source mapping must be enabled.

Verify:

```powershell
aws lambda list-event-source-mappings `
  --function-name backblaze-dev-s3-event-handler `
  --query 'EventSourceMappings[].{UUID:UUID,State:State,BatchSize:BatchSize,EventSourceArn:EventSourceArn}' `
  --output table
```

Expected:

```text
State     = Enabled
BatchSize = 1
```

The BatchSize of `1` matches the pipeline's file-level processing ownership model.

---

# 🔎 Final Infrastructure Audit

From the repository root:

```powershell
cd C:\Users\ASUS\Desktop\backblaze-aws-lakehouse
```

Check Git:

```powershell
git status --short
git log -1 --oneline
```

A deployment release should leave the working tree clean.

Check Terraform:

```powershell
cd .\terraform\environments\dev

terraform plan
```

Expected:

```text
No changes. Your infrastructure matches the configuration.
```

---

# 🔍 Verify Lambda

```powershell
aws lambda get-function `
  --function-name backblaze-dev-s3-event-handler `
  --qualifier <PUBLISHED_VERSION> `
  --query '{FunctionName:Configuration.FunctionName,Version:Configuration.Version,Runtime:Configuration.Runtime,Handler:Configuration.Handler,CodeSha256:Configuration.CodeSha256}' `
  --output table
```

---

# 🔍 Verify Glue

For each Glue job:

```powershell
aws glue get-job `
  --job-name <JOB_NAME> `
  --query 'Job.[Name,GlueVersion,WorkerType,NumberOfWorkers,Timeout,MaxRetries,ExecutionClass,Command.ScriptLocation]' `
  --output json
```

The `ScriptLocation` must point to the immutable deployment artifact generated by GitHub Actions.

---

# 🔍 Verify Step Functions

```powershell
aws stepfunctions describe-state-machine `
  --state-machine-arn arn:aws:states:ap-south-1:131912110087:stateMachine:backblaze-dev-file-processing `
  --query '{Name:name,Status:status,Type:type}' `
  --output table
```

Expected:

```text
Status = ACTIVE
Type   = STANDARD
```

---

# 🧾 Actual Deployment Evidence

The deployed environment was verified on:

```text
2026-09-21
```

The verification included:

```text
Terraform plan
    -> No changes

Lambda
    -> Published version 2

Glue CI/CD
    -> Successful

Glue jobs
    -> All 8 ScriptLocations point to immutable deployment artifacts

Step Functions
    -> ACTIVE
    -> STANDARD

S3 -> SQS
    -> Configured

SQS -> Lambda
    -> Enabled

Git repository
    -> Clean
```

The verified Lambda deployment was:

```text
Lambda version: 2
```

The Glue deployment used immutable artifacts under the:

```text
glue/
```

prefix.

This is deployment evidence, not an assumption that the runtime pipeline processed a real file successfully.

---

# 🔁 Rollback Strategy

## Lambda Rollback

Lambda releases are published as numbered versions.

A rollback uses a previously verified published version and its immutable S3 artifact.

The deployment model deliberately does not depend on `$LATEST`.

The current workflow does not manage a Lambda alias, so rollback is performed against a known published version.

---

## Glue Rollback

Glue scripts are deployed to immutable S3 paths.

A rollback therefore points the Glue job back to a previously verified artifact.

The repository-based process is:

```text
Identify known-good Git commit
        |
        v
Restore or revert code
        |
        v
Run Deploy Glue
        |
        v
Verify all 8 ScriptLocations
```

Previously generated deployment artifacts should be retained for emergency rollback.

---

## Terraform Rollback

Do not use:

```powershell
terraform -lock=false
```

Do not use Terraform to force application-code rollback when GitHub Actions owns the application artifact.

Terraform should continue to manage infrastructure.

GitHub Actions should continue to manage Lambda and Glue application releases.

---

# 🧪 Deployment vs Runtime Testing

A successful deployment proves that:

```text
Infrastructure configuration
+
Application artifacts
+
AWS wiring
```

are deployed correctly.

It does **not** prove that:

```text
S3 object
    ->
SQS event
    ->
Lambda
    ->
DynamoDB
    ->
Step Functions
    ->
Glue Bronze
    ->
Glue Silver
    ->
DQ
    ->
Gold
```

has completed successfully.

Runtime validation is a separate testing stage.

---

# ⚠️ Known Runtime Reliability Finding

During runtime testing, an externally aborted Step Functions execution left the DynamoDB control-plane record in:

```text
PROCESSING
```

The execution had been aborted while a Glue stage was running.

Because the normal state-machine failure path was not reached, the cleanup transition did not execute.

This is a runtime recovery and reconciliation issue.

It is separate from infrastructure deployment.

The deployment process must not manually modify the runtime control-plane state simply to make an infrastructure audit pass.

---

# 🧑‍💻 Normal Developer Workflow

After the project is deployed, normal development follows this model.

### Infrastructure change

Modify Terraform:

```text
terraform/environments/dev/
```

Then:

```powershell
terraform fmt
terraform validate
terraform plan
terraform apply
terraform plan
```

The final plan should return:

```text
No changes. Your infrastructure matches the configuration.
```

---

### Lambda application change

Modify:

```text
lambda/s3_event_handler/lambda_function.py
```

Commit the change:

```powershell
git add lambda/s3_event_handler/lambda_function.py
git commit -m "Update Lambda event handler"
git push origin main
```

Then manually trigger:

```text
Deploy Lambda
```

from GitHub Actions.

The workflow creates an immutable artifact and publishes a new Lambda version.

---

### Glue application change

Modify one or more scripts under:

```text
glue_scripts/scripts/
```

Commit and push:

```powershell
git add glue_scripts/
git commit -m "Update Glue pipeline"
git push origin main
```

Then manually trigger:

```text
Deploy Glue
```

from GitHub Actions.

The workflow validates exactly eight scripts, uploads immutable artifacts, updates the Glue jobs, and verifies every ScriptLocation.

---

# 🚫 What Developers Should Not Do

Do not:

```text
Put AWS access keys into GitHub
Run terraform -lock=false
Manually edit the deployed Glue ScriptLocation
Upload Lambda ZIPs manually and bypass CI/CD
Overwrite an existing deployment artifact
Use Terraform to deploy application-code releases
Run Terraform from an arbitrary directory
Apply an unexpected Terraform plan
Run a historical 229 GB backfill as part of Terraform deployment
Start real Glue processing only to prove infrastructure deployment
Manually change DynamoDB runtime state to hide a failed test
```

---

# ✅ Deployment Completion Criteria

A deployment release is complete when:

```text
[ ] Repository is clean
[ ] AWS identity is correct
[ ] Terraform initialization succeeds
[ ] Terraform validation succeeds
[ ] Terraform plan has no unexpected changes
[ ] Terraform apply completes when infrastructure changed
[ ] Post-apply Terraform plan is clean
[ ] GitHub OIDC authentication succeeds
[ ] Lambda workflow succeeds
[ ] Lambda published version is verified
[ ] Lambda CodeSha256 is verified
[ ] Glue workflow validates exactly 8 scripts
[ ] Glue workflow succeeds
[ ] All 8 Glue ScriptLocations are verified
[ ] Step Functions is ACTIVE
[ ] Step Functions is STANDARD
[ ] Repository ASL matches deployed definition
[ ] S3 -> SQS configuration is verified
[ ] SQS -> Lambda mapping is enabled
[ ] Deployment artifacts are immutable
[ ] Deployment evidence is recorded
```

---

# 🧭 What a New Engineer Actually Does

The repository is designed so that a new engineer does not need to manually create every AWS resource.

The intended process is:

```text
1. Clone repository
        |
        v
2. Configure AWS CLI
        |
        v
3. Verify AWS account
        |
        v
4. Ensure Terraform backend/bootstrap exists
        |
        v
5. terraform init
        |
        v
6. terraform validate
        |
        v
7. terraform plan
        |
        v
8. Review changes
        |
        v
9. terraform apply
        |
        v
10. Verify Terraform is clean
        |
        v
11. Configure/verify GitHub OIDC
        |
        v
12. Run Deploy Lambda
        |
        v
13. Run Deploy Glue
        |
        v
14. Verify Step Functions
        |
        v
15. Verify S3 -> SQS -> Lambda wiring
        |
        v
16. Run runtime testing separately
```

The important distinction is:

```text
Clone
  ≠
Immediately terraform apply in every AWS account
```

The repository contains environment-specific identifiers such as:

```text
AWS account ID
S3 bucket names
Terraform state bucket
GitHub repository identity
IAM role names
```

Therefore, deploying the same project into a different AWS account requires changing those environment-specific values and the GitHub OIDC trust configuration.

For the existing project environment, the repository provides the Terraform configuration and CI/CD implementation required to reproduce the deployed architecture without manually recreating each AWS resource.

---

# 🏁 Deployment Model

The final deployment model is:

```text
                GitHub Repository
                       |
          +------------+-------------+
          |                          |
          v                          v
     Terraform                  GitHub Actions
          |                          |
          |                          +---- AWS OIDC
          |                          |
          |                +---------+---------+
          |                |                   |
          |                v                   v
          |             Lambda             Glue scripts
          |                |                   |
          |                v                   v
          |          S3 artifact          S3 artifacts
          |                |                   |
          |                v                   v
          |        Published version       Glue jobs
          |
          +-------------------------------+
          |
          +---- S3
          +---- SQS
          +---- DynamoDB
          +---- Step Functions
          +---- IAM
          +---- SNS
```

This gives the project:

```text
Infrastructure as Code
+
OIDC-based AWS authentication
+
Immutable application artifacts
+
Versioned Lambda releases
+
Versioned Glue releases
+
Terraform drift detection
+
Explicit ownership boundaries
+
Deployment verification
+
Rollback capability
```

The result is not a manual AWS-console deployment.

The repository is the source of truth for infrastructure and application code, Terraform is the source of truth for infrastructure, and GitHub Actions is the release mechanism for Lambda and Glue application artifacts.
