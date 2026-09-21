# **BACKBLAZE AWS MODERN DATA LAKEHOUSE PROJECT**

<p align="center">
  <table>
    <tr>
      <td align="center" width="95">
        <img src="https://img.shields.io/badge/Backblaze-E11B22?style=for-the-badge&logo=backblaze&logoColor=white" alt="Backblaze" height="32"/><br/>
        <sub><b>Backblaze</b></sub>
      </td>
      <td align="center" width="95">
        <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="Python" width="42" height="42"/><br/>
        <sub><b>Python</b></sub>
      </td>
      <td align="center" width="95">
        <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/apachespark/apachespark-original.svg" alt="Apache Spark" width="42" height="42"/><br/>
        <sub><b>Apache Spark</b></sub>
      </td>
      <td align="center" width="95">
        <img src="https://img.shields.io/badge/Apache%20Iceberg-008080?style=for-the-badge&logo=apache&logoColor=white" alt="Apache Iceberg" height="32"/><br/>
        <sub><b>Apache Iceberg</b></sub>
      </td>
      <td align="center" width="95">
        <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg" alt="AWS" width="48" height="48"/><br/>
        <sub><b>AWS</b></sub>
      </td>
      <td align="center" width="95">
        <img src="https://img.shields.io/badge/Amazon_S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white" alt="Amazon S3" height="32"/><br/>
        <sub><b>Amazon S3</b></sub>
      </td>
      <td align="center" width="95">
        <img src="https://www.vectorlogo.zone/logos/amazon_aws/amazon_aws-icon.svg" alt="AWS IAM" width="42" height="42"/><br/>
        <sub><b>AWS IAM</b></sub>
      </td>
      <td align="center" width="95">
        <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/githubactions/githubactions-original.svg" alt="GitHub Actions" width="42" height="42"/><br/>
        <sub><b>GitHub Actions</b></sub>
      </td>
      <td align="center" width="95">
        <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/terraform/terraform-original.svg" alt="Terraform" width="42" height="42"/><br/>
        <sub><b>Terraform</b></sub>
      </td>
    </tr>
  </table>
</p>

<p align="center">
  <b>Production-oriented AWS Lakehouse for Backblaze Drive Stats</b><br/>
  Historical backfill • Event-driven incremental ingestion • Apache Iceberg • AWS Glue • Lambda • SQS • DynamoDB • Step Functions • Terraform • GitHub Actions
</p>

<p align="center">
  <a href="DEPLOYMENT.md">📚 Deployment Runbook</a> •
  <a href="architecture/project%20architecture.svg">🏗️ Architecture</a> •
  <a href="stepfunctions/backblaze_file_processing.asl.json">⚙️ Step Functions</a>
</p>

---
# 1. 📌 What Problem Does This Project Solve?

The Backblaze Drive Stats dataset contains large amounts of hard-drive statistics collected over time.

The data is useful for analytics, but building a reliable pipeline around it creates several practical engineering problems.

The pipeline needs to answer questions such as:

- How do we load a large historical dataset without treating every run like a completely new pipeline?
- How do we process a new CSV when it arrives without manually starting a Glue job?
- What happens when the same S3 event is delivered more than once?
- How do we know which file is currently being processed?
- How do we prevent two files from being processed at the same time when the pipeline is designed around one active processing unit?
- Where do we preserve the original source data?
- Where do we standardize changing source schemas?
- What happens when records fail validation?
- How do we coordinate Bronze → Silver → DQ → Gold?
- What happens when one processing stage fails?
- How do we resume processing after a failure?
- How do we deploy infrastructure and application code without manually rebuilding everything?

This project is designed to solve those problems as one system.

The result is not simply:

```text
CSV → Spark → Table
```

pipeline.

The objective is to understand how a modern data platform behaves when it must support:

- large historical backfills
- incremental ingestion
- event-driven processing
- schema evolution
- data-quality enforcement
- quarantine
- idempotency
- orchestration state
- failure handling
- recovery
- infrastructure as code
- CI/CD
- immutable application releases
- operational verification
- cost-aware execution

The platform is divided into two major planes:

```text
┌─────────────────────────────────────────────────────────────┐
│                         DATA PLANE                          │
│                                                             │
│  Backblaze / Source Files                                   │
│              │                                              │
│              ▼                                              │
│        Amazon S3 RAW                                        │
│              │                                              │
│              ▼                                              │
│        AWS Glue / Spark                                     │
│              │                                              │
│              ▼                                              │
│      Bronze → Silver → DQ → Gold                            │
│              │                                              │
│              ▼                                              │
│        Apache Iceberg                                       │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                       CONTROL PLANE                         │
│                                                             │
│  S3 ObjectCreated                                           │
│          │                                                  │
│          ▼                                                  │
│         SQS                                                 │
│          │                                                  │
│          ▼                                                  │
│       Lambda                                                │
│          │                                                  │
│          ▼                                                  │
│   DynamoDB Control Plane                                    │
│          │                                                  │
│          ▼                                                  │
│   Step Functions Standard                                   │
│          │                                                  │
│          ▼                                                  │
│       Glue Jobs                                              │
└─────────────────────────────────────────────────────────────┘
```

The architecture deliberately separates data processing from orchestration and state management.

---

# 🎯 Project Goals

The main goals of this project are:

- Build an AWS-native lakehouse using **Apache Iceberg**.
- Process historical Backblaze Drive Stats releases in a controlled way.
- Transition from historical processing to event-driven incremental ingestion.
- Preserve source fidelity in Bronze.
- Apply canonical schema governance in Silver.
- Handle schema evolution across historical releases.
- Enforce data quality before downstream consumption.
- Quarantine invalid data.
- Produce analytical Gold datasets.
- Make file processing idempotent.
- Maintain explicit pipeline state using DynamoDB.
- Orchestrate processing stages with AWS Step Functions.
- Deploy infrastructure using Terraform.
- Deploy Lambda and Glue application code using GitHub Actions.
- Use GitHub OIDC instead of long-lived AWS credentials.
- Keep application deployment artifacts immutable and traceable.
- Document failure modes instead of hiding them.

---

# 🏗️ Project Architecture

## Primary Architecture


<p align="center">
  <img
    src="architecture/project architecuture.svg"
    width="100%"
    alt="Backblaze AWS Lakehouse Architecture"
  />
</p>

---

# 🔄 End-to-End Data Flow

The project contains two major processing paths.

## 🏛️ Historical Processing Path

```text
Backblaze Drive Stats
        │
        ▼
Amazon S3 RAW
        │
        ▼
AWS Glue / Spark
        │
        ▼
Bronze Iceberg
        │
        ▼
Silver Iceberg
        │
        ▼
Data Quality
        │
        ▼
Gold Iceberg
        │
        ▼
Athena / Analytics
```

Historical processing is release-scoped and controlled.

Terraform does **not** launch the historical backfill.

This keeps:

```text
Infrastructure provisioning
        ≠
Historical data processing
```

---

## ⚡ Event-Driven Incremental Path

```text
New CSV Object
      │
      ▼
 Amazon S3 RAW
      │
      │ ObjectCreated
      ▼
     SQS
      │
      ▼
   Lambda
      │
      ├── Validate event
      ├── Validate bucket/prefix
      ├── Parse release metadata
      ├── Register file idempotently
      └── Start/recover orchestration
               │
               ▼
       DynamoDB Control Plane
               │
               ▼
      Step Functions Standard
               │
               ├── Claim File
               │
               ├── Bronze
               │
               ├── Silver
               │
               ├── Data Quality
               │
               └── Gold
               │
               ▼
        DynamoDB → SUCCESS / IDLE
```

The incremental model is file-scoped:

```text
1 S3 Object
    ↓
1 Event
    ↓
1 File Registration
    ↓
1 Processing Unit
    ↓
1 Orchestration Run
```

There is no expected-next-date rule.

---

# 🧠 Data Plane vs Control Plane

## Data Plane

```text
S3 RAW
  │
  ▼
Bronze
  │
  ▼
Silver
  │
  ▼
Data Quality
  │
  ▼
Gold
```

The data plane answers:

> **What should the data look like?**

---

## Control Plane

```text
S3 Event
   │
   ▼
SQS
   │
   ▼
Lambda
   │
   ▼
DynamoDB
   │
   ▼
Step Functions
   │
   ▼
Glue
```

The control plane answers:

> **What is being processed, what has already been processed, and what should happen next?**

---

# 🪜 Lakehouse Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│                            GOLD                              │
│              Analytical / business-ready data               │
├──────────────────────────────────────────────────────────────┤
│                             DQ                               │
│            Data quality / acceptance / quarantine             │
├──────────────────────────────────────────────────────────────┤
│                           SILVER                             │
│  Canonical schema / casting / deduplication / evolution      │
├──────────────────────────────────────────────────────────────┤
│                           BRONZE                             │
│          Source-oriented / lineage / ingestion               │
├──────────────────────────────────────────────────────────────┤
│                            RAW                               │
│                   Original source landing                    │
└──────────────────────────────────────────────────────────────┘
```

---

# 🥉 Bronze Layer

Bronze is intentionally simple.

Its responsibility is to preserve source data as faithfully as practical while adding the metadata required for lineage and processing control.

Typical metadata includes:

```text
release_id
ingested_at
processing_run_id
```

The intended pattern is:

```text
RAW
 ↓
Bronze
 ↓
Preserve source
```

Business canonicalization does not belong in Bronze.

---

# 🥈 Silver Layer

Silver is the controlled source-to-canonical boundary.

Responsibilities include:

- schema mapping
- type casting
- canonicalization
- required-field handling
- deduplication
- schema evolution
- quarantine
- deterministic merge behavior

Conceptually:

```text
Bronze
   │
   ▼
Canonical Schema
   │
   ├── Type Casting
   ├── Column Mapping
   ├── Required Field Checks
   ├── Deduplication
   └── Schema Evolution
             │
             ▼
        Silver Iceberg
```

---

# 🧪 Data Quality Layer

The Data Quality stage prevents invalid records from silently flowing into Gold.

Representative checks include:

- missing required fields
- malformed values
- invalid data types
- duplicate records
- schema mismatches
- unexpected source structures

Desired behavior:

```text
Invalid Record
      │
      ▼
DQ Evaluation
      │
      ├── Accepted
      │
      └── Quarantine / Failure
```

---

# 🥇 Gold Layer

Gold contains analytical datasets intended for downstream consumption.

Gold is intentionally downstream of:

```text
Bronze
   ↓
Silver
   ↓
Data Quality
```

This keeps analytical consumers isolated from raw source irregularities.

---

# 🧊 Apache Iceberg

Apache Iceberg is the table format used by the lakehouse.

The project uses Iceberg for capabilities such as:

- transactional table operations
- snapshots
- schema evolution
- partition evolution
- time travel
- table metadata management

Conceptually:

```text
Object Storage
      │
      ▼
Amazon S3
      │
      ▼
Apache Iceberg
      │
      ├── Bronze
      ├── Silver
      └── Gold
```

## Iceberg Table Format

<p align="center">
  <img
    src="docs/iceberge_table_formate.png"
    width="90%"
    alt="Apache Iceberg Table Format"
  />
</p>

---

# 🎛️ Control Plane Components

## ☁️ Amazon S3

S3 is the durable landing zone and event source.

Raw data follows:

```text
raw/drivestats/
```

Responsibilities:

- raw source storage
- event generation
- durable object storage
- lakehouse storage backend

---

## 📨 Amazon SQS

SQS decouples object arrival from processing.

Benefits:

- buffering
- asynchronous processing
- retries
- dead-letter handling
- burst protection
- loose coupling

The event-source mapping uses:

```text
BatchSize = 1
```

so each event can remain an explicit processing unit.

---

## ⚡ AWS Lambda

Lambda is intentionally thin.

It does not process large datasets.

Responsibilities:

```text
Receive SQS Event
      ↓
Parse S3 Event
      ↓
Validate Bucket / Prefix / Object
      ↓
Extract Release/File Metadata
      ↓
Register File Idempotently
      ↓
Start/Recover Step Functions
```

---

## 🗃️ Amazon DynamoDB

DynamoDB is the control-plane state store.

It tracks:

```text
Pipeline state
Active file
File status
Processing run ID
Processing timestamps
Queue information
```

Pipeline identity:

```text
PIPELINE#BACKBLAZE
```

File-level identity:

```text
FILE#<source-file>
```

DynamoDB is not the analytical store.

Its purpose is:

```text
Coordination
+
Idempotency
+
Operational state
```

---

## 🔁 AWS Step Functions

Step Functions coordinates the processing lifecycle.

Major sequence:

```text
Determine Mode
      ↓
Find Pending File
      ↓
Claim File
      ↓
Run Bronze
      ↓
Run Silver
      ↓
Run DQ
      ↓
Run Gold
      ↓
Mark Success / Release
```

The workflow definition is:

```text
stepfunctions/backblaze_file_processing.asl.json
```

---

# 🔀 Processing Modes

## Historical Backfill

Historical processing is release-scoped.

Example:

```text
--release_id data_Q1_2026
```

Full-load jobs:

```text
bronze_ingestion
silver_cleaned
data_quality_check
gold_layer
```

Terraform does not automatically launch historical processing.

---

## Incremental Processing

Incremental processing is file-scoped.

Jobs:

```text
bronze_layer
silver_layer
data_quality_layer
gold_analytics_layer
```

Bronze and Silver receive:

```text
--input_path
--release_id
```

Gold receives:

```text
--release_id
```

---

# 🔐 Idempotency

A duplicate event must not become duplicate processing.

The registration model uses a deterministic file identity:

```text
FILE#s3://.../raw/drivestats/...
```

The pipeline control record:

```text
PIPELINE#BACKBLAZE
```

coordinates the active processing lock.

The intended invariant is:

```text
At most one active processing unit
for the pipeline control lock.
```

---

# 🧬 Schema Evolution

Backblaze historical releases can change schema.

The project separates:

```text
Source Schema
      ↓
Bronze Preservation
      ↓
Canonical Schema
      ↓
Silver Governance
```

Schema artifacts are maintained in:

```text
schemas/
```

---

# ⚙️ AWS Glue / Spark

AWS Glue provides managed Spark execution.

## Historical Jobs

| Job | Responsibility |
|---|---|
| `bronze_ingestion` | Historical Bronze ingestion |
| `silver_cleaned` | Historical Silver processing |
| `data_quality_check` | Historical DQ |
| `gold_layer` | Historical Gold |

## Incremental Jobs

| Job | Responsibility |
|---|---|
| `bronze_layer` | File-scoped Bronze |
| `silver_layer` | File-scoped Silver |
| `data_quality_layer` | Incremental DQ |
| `gold_analytics_layer` | Incremental Gold |

---

# 💰 Compute Strategy

The project separates compute sizing between historical and incremental workloads.

Current development configuration:

| Job | Glue Version | Worker Type | Workers | Execution |
|---|---:|---|---:|---|
| `bronze_ingestion` | 6.0 | G.1X | 5 | FLEX |
| `silver_cleaned` | 5.1 | G.1X | 4 | STANDARD |
| `data_quality_check` | 6.0 | G.1X | 4 | STANDARD |
| `gold_layer` | 6.0 | G.1X | 4 | STANDARD |
| `bronze_layer` | 5.1 | G.1X | 2 | STANDARD |
| `silver_layer` | 5.1 | G.1X | 2 | STANDARD |
| `data_quality_layer` | 5.1 | G.1X | 2 | STANDARD |
| `gold_analytics_layer` | 6.0 | G.1X | 2 | STANDARD |

Common job settings include:

```text
Timeout: 480 minutes
MaxRetries: 0
Concurrency: 1
```

These values are deployment-specific and should be reviewed for another AWS account.

---

# ☁️ Infrastructure as Code

Terraform manages infrastructure under:

```text
terraform/environments/dev/
```

Managed components include:

```text
S3
SQS
DynamoDB
Lambda
Lambda Event Source Mapping
Step Functions
SNS
IAM
GitHub OIDC
Glue Jobs
Deployment Artifact Storage
```

Terraform state is remote and excluded from Git.

Do not use:

```text
terraform -lock=false
```

during normal deployment or recovery.

---

# 🔁 CI/CD

Infrastructure and application releases are separate.

```text
Terraform
    ↓
Infrastructure

GitHub Actions
    ↓
Application releases
```

Main workflows:

```text
.github/workflows/deploy-lambda.yml
.github/workflows/deploy-glue.yml
```

GitHub Actions authenticates to AWS through GitHub OIDC.

---

# ⚡ Lambda CI/CD

Deployment flow:

```text
Checkout
   ↓
Python validation
   ↓
GitHub OIDC
   ↓
AWS identity verification
   ↓
Build ZIP
   ↓
Upload immutable artifact
   ↓
Update Lambda
   ↓
Publish version
   ↓
Verify checksum/configuration
```

Lambda artifacts are stored using commit-oriented paths.

---

# ⚙️ Glue CI/CD

Deployment flow:

```text
Checkout
   ↓
Validate exactly 8 Glue scripts
   ↓
GitHub OIDC
   ↓
Upload immutable artifacts
   ↓
Read current Glue Job
   ↓
Preserve existing configuration
   ↓
Update ScriptLocation
   ↓
Verify all 8 jobs
```

The workflow preserves:

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

and changes the application script location.

---

# 🔐 Security Model

## GitHub Authentication

GitHub Actions uses:

```text
GitHub OIDC
```

instead of long-lived AWS access keys.

The trust policy should be restricted to the intended repository and branch.

---

## S3 Security

The lakehouse bucket uses:

```text
Public Access Block
SSE-S3 Encryption
```

---

## SQS Security

The main queue uses a DLQ and redrive policy.

---

## IAM

Glue deployment uses:

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

Do not replace this with broad administrator permissions.

---

# 🔒 Repository Security Audit

The repository was audited for:

- AWS access-key patterns
- AWS secret-key patterns
- GitHub token patterns
- private-key markers
- sensitive tracked filenames
- Terraform state
- `.env` files
- generated deployment artifacts

No evidence of exposed AWS credentials or private keys was found in the checks performed.

The repository `.gitignore` excludes:

```text
.terraform/
*.tfstate
*.tfstate.*
*.tfvars
*.tfvars.json
.env
.env.*
*.pem
*.key
.aws/
credentials
__pycache__/
*.py[cod]
.venv/
deployment/artifacts/
```

---

# 📸 Architecture & Project Evidence

## Step Functions Processing Graph

<p align="center">
  <img
    src="architecture/stepfunctions_graph.png"
    width="95%"
    alt="Step Functions Processing Graph"
  />
</p>

## QuickSight Dashboard

<p align="center">
  <img
    src="dashboard/QuickSight.png"
    width="95%"
    alt="QuickSight Dashboard"
  />
</p>

## Cost Analysis

<p align="center">
  <img
    src="docs/cost_analysis.png"
    width="95%"
    alt="AWS Lakehouse Cost Analysis"
  />
</p>

## Apache Iceberg Table Format

<p align="center">
  <img
    src="docs/iceberge_table_formate.png"
    width="90%"
    alt="Apache Iceberg Table Format"
  />
</p>

---

# 📈 Historical Validation Evidence

A historical Silver-layer validation was performed against the 2013 release.

The verified Silver schema included:

```text
snapshot_date
drive_serial_number
drive_model
capacity_bytes
failure_flag
SMART raw/normalized columns
release_id
lineage timestamps / IDs
```

Observed 2013 `failure_flag` distribution:

```text
failure_flag = 0 → 5,090,777
failure_flag = 1 → 724
```

This is historical validation evidence and does not represent the current runtime state of another deployment.

---

# 📁 Repository Structure

```text
backblaze-aws-lakehouse/
│
├── .github/
│   └── workflows/
│       ├── deploy-lambda.yml
│       └── deploy-glue.yml
│
├── architecture/
│   ├── project architecture.svg
│   ├── project architecture.png
│   └── stepfunctions_graph.png
│
├── dashboard/
│   └── QuickSight.png
│
├── deployment/
│   └── inventory/
│
├── docs/
│   ├── cost_analysis.png
│   └── iceberge_table_formate.png
│
├── glue/
│
├── glue_scripts/
│   └── scripts/
│       ├── full-load/
│       └── incremental-load/
│
├── lambda/
│   └── s3_event_handler/
│       └── lambda_function.py
│
├── schemas/
│
├── stepfunctions/
│   └── backblaze_file_processing.asl.json
│
├── terraform/
│   └── environments/
│       └── dev/
│
├── .gitignore
├── DEPLOYMENT.md
└── README.md
```

---

# 🧰 Technology Stack

| Technology | Role |
|---|---|
| **Python** | Lambda and Glue application development |
| **Apache Spark** | Distributed ETL engine |
| **AWS Glue** | Managed Spark execution |
| **Apache Iceberg** | Lakehouse table format |
| **Amazon S3** | Raw and lakehouse storage |
| **Amazon SQS** | Event buffering |
| **AWS Lambda** | Event validation and orchestration trigger |
| **Amazon DynamoDB** | Control-plane state and idempotency |
| **AWS Step Functions** | Workflow orchestration |
| **Amazon SNS** | Notification layer |
| **AWS IAM** | Access control |
| **GitHub Actions** | CI/CD |
| **GitHub OIDC** | Short-lived AWS authentication |
| **Terraform** | Infrastructure as Code |
| **Amazon Athena** | Lakehouse querying |
| **Amazon QuickSight** | Visualization |

---

# 💡 Engineering Decisions

## Why separate data plane and control plane?

The data plane answers:

```text
How should the data be transformed?
```

The control plane answers:

```text
What is running?
What finished?
What failed?
What should run next?
```

Keeping those responsibilities separate improves observability and recovery.

---

## Why keep Bronze simple?

Bronze should remain close to the source.

Putting business logic into Bronze increases coupling and makes replay and debugging more difficult.

---

## Why use Iceberg?

Iceberg provides the table abstraction required for transactional and evolvable lakehouse tables.

---

## Why use SQS?

SQS decouples object arrival from pipeline execution and provides buffering and retry semantics.

---

## Why use Lambda?

Lambda handles lightweight event processing.

Glue handles the heavy distributed workload.

```text
Lambda
   =
Event Handler

Glue
   =
Data Processor
```

---

## Why use DynamoDB?

The system needs explicit control-plane state.

DynamoDB tracks:

```text
file
run
stage
status
active processing state
```

---

## Why use Step Functions?

The pipeline contains dependent stages:

```text
Bronze
  ↓
Silver
  ↓
DQ
  ↓
Gold
```

Step Functions makes those dependencies explicit.

---

## Why use GitHub OIDC?

OIDC removes the need for long-lived AWS credentials inside GitHub.

---

## Why separate Terraform and application deployment?

Terraform owns infrastructure.

GitHub Actions owns application release artifacts.

This prevents application code from being overwritten during infrastructure reconciliation.

---

# 🧪 Testing Strategy

The project uses multiple testing layers.

## Infrastructure Test

```text
terraform plan
```

Expected:

```text
No changes.
Your infrastructure matches the configuration.
```

---

## Event Path Test

Verify:

```text
S3
 ↓
SQS
 ↓
Lambda
```

---

## Orchestration Test

Verify:

```text
Lambda
 ↓
DynamoDB
 ↓
Step Functions
```

---

## Processing Test

Verify:

```text
Bronze
 ↓
Silver
 ↓
DQ
 ↓
Gold
```

---

## Recovery Test

Verify that a failure preserves enough state to identify:

```text
failed file
failed run
failed stage
```

and that the intended resume behavior works.

---

# 🚧 Known Runtime Reliability Finding

A previous runtime test externally aborted a Step Functions execution while a Glue stage was running.

Observed:

```text
Step Functions = ABORTED
Running executions = 0
SQS = empty
DynamoDB = PROCESSING
```

This exposed a control-plane recovery gap.

An externally aborted execution can bypass the normal state-machine failure path that performs cleanup.

This is a:

```text
Runtime Recovery / Reconciliation
```

problem, not an infrastructure deployment problem.

The issue is documented in:

```text
DEPLOYMENT.md
```

The architectural distinction is:

```text
Infrastructure deployment correctness
                ≠
Runtime recovery correctness
```

---

# 🔧 Rollback Strategy

## Lambda

Lambda deployments use published versions.

Rollback should restore a previously verified published version and its immutable artifact.

Do not rely on `$LATEST` for emergency rollback.

---

## Glue

Glue application scripts are deployed to immutable S3 artifact locations.

Rollback should point jobs back to known-good application artifacts.

---

## Terraform

Terraform is responsible for infrastructure rollback.

Application rollback remains under the application deployment workflows.

---

# 💸 FinOps Principles

The project considers cost during design.

Key principles:

- historical and incremental workloads use different compute profiles
- incremental workloads use smaller Glue jobs
- historical backfills are not launched by Terraform
- concurrency is intentionally controlled
- file-scoped processing avoids unnecessary full refreshes
- immutable artifacts make deployments traceable
- unnecessary all-purpose compute is avoided
- data layout should support pruning and efficient reads

The objective is not only:

```text
Make it work
```

but:

```text
Make it work reliably and economically
```

---

# 🚀 Getting Started

> This section is written for a new engineer deploying the project into **their own AWS account**.
>
> The existing AWS environment, S3 data, Terraform state, Lambda artifacts, Glue artifacts, and DynamoDB state are **not included in this repository**.

---

# 📋 Prerequisites

Install:

```text
Git
AWS CLI
Terraform
Python 3.x
```

Recommended:

```text
Terraform 1.16.x
AWS CLI 2.x
Git 2.x
```

You also need:

```text
AWS Account
GitHub Repository
Backblaze Drive Stats dataset
```

---

# 1️⃣ Clone the Repository

```bash
git clone https://github.com/Abdulkhadir63/backblaze-aws-lakehouse.git

cd backblaze-aws-lakehouse
```

Verify:

```bash
git status
```

---

# 2️⃣ Configure AWS

Authenticate using your preferred AWS method.

Verify:

```bash
aws sts get-caller-identity
```

The returned account should be the account where you intend to deploy the project.

---

# 3️⃣ Select AWS Region

The original environment uses:

```text
ap-south-1
```

Set the region:

```bash
aws configure set region ap-south-1
```

Verify:

```bash
aws configure get region
```

---

# 4️⃣ Create Terraform State Storage

Create a dedicated S3 bucket for Terraform state in your own account.

Recommended properties:

```text
Versioning = Enabled
Public Access = Blocked
Encryption = Enabled
```

Example:

```text
my-backblaze-terraform-state-<account-id>
```

Do not use another person's state bucket.

---

# 5️⃣ Configure Terraform Backend

Open:

```text
terraform/environments/dev/backend.tf
```

Replace the backend values with your own:

```text
your state bucket
your AWS region
your backend key
```

Terraform state must remain outside Git.

---

# 6️⃣ Replace Account-Specific Configuration

Search for values from the original development account:

```powershell
git grep -n "131912110087"
```

Search for original deployment bucket names:

```powershell
git grep -n "backblaze-de-lakehouse-project"

git grep -n "backblaze-dev-deployment-artifacts"

git grep -n "backblaze-de-terraform-state"
```

Replace account-specific values before deploying into another account.

Do not copy another person's Terraform state.

---

# 7️⃣ Configure GitHub OIDC

Configure:

```text
GitHub OIDC Provider
        ↓
IAM Role
        ↓
Trust Policy
        ↓
Repository + Branch Restriction
```

Trust should be restricted to your own repository:

```text
repo:YOUR_OWNER/YOUR_REPOSITORY:ref:refs/heads/main
```

Do not use broad wildcard repository trust.

---

# 8️⃣ Configure GitHub Deployment Permissions

The GitHub Actions deployment role needs the appropriate permissions for:

### Lambda

```text
S3 artifact upload
Lambda deployment
Lambda verification
```

### Glue

```text
S3 artifact upload
glue:GetJob
glue:UpdateJob
iam:PassRole
```

The Glue role should only be passable to Glue.

---

# 9️⃣ Initialize Terraform

Enter the environment:

```powershell
cd terraform\environments\dev
```

Initialize:

```powershell
terraform init
```

If the backend configuration changed:

```powershell
terraform init -reconfigure
```

---

# 🔟 Validate Terraform

```powershell
terraform validate
```

---

# 1️⃣1️⃣ Review Terraform Plan

```powershell
terraform plan
```

Do not apply unexpected destructive changes.

Look carefully for:

```text
resource replacement
bucket destruction
IAM replacement
state backend changes
unexpected Glue changes
```

---

# 1️⃣2️⃣ Apply Infrastructure

Only after reviewing the plan:

```powershell
terraform apply
```

Review and approve the expected changes.

---

# 1️⃣3️⃣ Reconcile Terraform

After deployment:

```powershell
terraform plan
```

Expected:

```text
No changes.
Your infrastructure matches the configuration.
```

---

# 1️⃣4️⃣ Deploy Lambda

GitHub:

```text
Repository
    ↓
Actions
    ↓
Deploy Lambda
    ↓
Run workflow
```

Workflow:

```text
.github/workflows/deploy-lambda.yml
```

The workflow validates, builds, uploads, deploys, publishes, and verifies the Lambda artifact.

---

# 1️⃣5️⃣ Deploy Glue

GitHub:

```text
Repository
    ↓
Actions
    ↓
Deploy Glue
    ↓
Run workflow
```

Workflow:

```text
.github/workflows/deploy-glue.yml
```

The workflow deploys all eight Glue scripts.

---

# 1️⃣6️⃣ Verify Lambda

```powershell
aws lambda list-versions-by-function `
  --function-name <YOUR_LAMBDA_NAME> `
  --query "Versions[].{Version:Version,CodeSha256:CodeSha256,LastModified:LastModified}" `
  --output table
```

A published version should exist.

---

# 1️⃣7️⃣ Verify Glue

```powershell
aws glue get-job `
  --job-name <JOB_NAME> `
  --query "Job.[Name,GlueVersion,WorkerType,NumberOfWorkers,Timeout,MaxRetries,ExecutionClass,Command.ScriptLocation]" `
  --output json
```

Run this for all eight jobs.

---

# 1️⃣8️⃣ Verify Step Functions

```powershell
aws stepfunctions describe-state-machine `
  --state-machine-arn <YOUR_STATE_MACHINE_ARN> `
  --query "{Name:name,Status:status,Type:type,RoleArn:roleArn}" `
  --output table
```

Expected:

```text
Status = ACTIVE
Type   = STANDARD
```

---

# 1️⃣9️⃣ Verify S3 → SQS

```powershell
aws s3api get-bucket-notification-configuration `
  --bucket <YOUR_RAW_BUCKET>
```

Expected:

```text
Event: s3:ObjectCreated:*
Prefix: raw/drivestats/
Suffix: .csv
Destination: SQS
```

---

# 2️⃣0️⃣ Verify SQS → Lambda

```powershell
aws lambda list-event-source-mappings `
  --function-name <YOUR_LAMBDA_NAME> `
  --query "EventSourceMappings[].{UUID:UUID,State:State,BatchSize:BatchSize,EventSourceArn:EventSourceArn}" `
  --output table
```

Expected:

```text
State     = Enabled
BatchSize = 1
```

---

# 2️⃣1️⃣ Prepare Test Data

Use a Backblaze Drive Stats CSV file.

Example:

```text
2026-03-31.csv
```

Do not commit raw datasets into Git.

---

# 2️⃣2️⃣ Upload Test Data

Example:

```powershell
aws s3 cp `
  "C:\path\to\2026-03-31.csv" `
  "s3://<YOUR_RAW_BUCKET>/raw/drivestats/data_Q1_2026/2026-03-31.csv"
```

Expected flow:

```text
S3 ObjectCreated
       ↓
SQS Message
       ↓
Lambda
```

---

# 2️⃣3️⃣ Verify SQS

```powershell
aws sqs get-queue-attributes `
  --queue-url <YOUR_MAIN_QUEUE_URL> `
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

---

# 2️⃣4️⃣ Verify Step Functions

```powershell
aws stepfunctions list-executions `
  --state-machine-arn <YOUR_STATE_MACHINE_ARN> `
  --status-filter RUNNING `
  --output table
```

A new execution should appear after successful event registration.

---

# 2️⃣5️⃣ Verify DynamoDB

```powershell
aws dynamodb get-item `
  --table-name <YOUR_CONTROL_TABLE> `
  --key '{"control_id":{"S":"PIPELINE#BACKBLAZE"}}'
```

During execution, the pipeline should show its processing state.

After successful completion, the control plane should release the processing lock.

---

# 2️⃣6️⃣ Verify Glue Processing

Check recent runs:

```powershell
aws glue get-job-runs `
  --job-name bronze_layer `
  --max-items 5 `
  --output table
```

Repeat for:

```text
silver_layer
data_quality_layer
gold_analytics_layer
```

---

# 2️⃣7️⃣ Verify Data Outputs

Confirm:

```text
Bronze output exists
        ↓
Silver output exists
        ↓
DQ succeeded
        ↓
Gold output exists
```

Use Athena or another supported query engine to validate the resulting Iceberg tables.

---

# 2️⃣8️⃣ End-to-End Success Criteria

A runtime test is successful only when:

```text
[ ] S3 ObjectCreated event generated
[ ] SQS received event
[ ] Lambda consumed event
[ ] DynamoDB registered file
[ ] Step Functions started
[ ] Bronze succeeded
[ ] Silver succeeded
[ ] Data Quality succeeded
[ ] Gold succeeded
[ ] Expected data exists
[ ] DynamoDB processing lock released
[ ] Main SQS is clear
[ ] DLQ is clear
```

Do not claim E2E success from:

```text
Terraform = No changes
```

alone.

---

# 🧪 Controlled Failure Test

After the happy path works, test a controlled failure.

Expected behavior:

```text
Glue Failure
      ↓
Step Functions catches failure
      ↓
Control-plane state records failure
      ↓
File remains identifiable
      ↓
Resume can continue from intended stage
```

---

# 🔄 Resume Semantics

The workflow contains explicit resume logic.

Conceptually:

```text
FAILED FILE
     ↓
Determine Resume Stage
     ↓
Resume Failed Stage
     ↓
Run Downstream Stages
```

The purpose is to avoid blindly rerunning the entire pipeline.

---

# 🚧 Runtime Recovery Finding

A previous runtime test externally aborted Step Functions while a Glue stage was running.

Observed:

```text
Step Functions = ABORTED
Running executions = 0
SQS = empty
DynamoDB = PROCESSING
```

This exposed a stale-processing recovery gap.

An externally aborted execution can bypass the normal state-machine failure path.

The next hardening step is a reconciliation mechanism that can identify stale processing safely.

This is documented separately in:

```text
DEPLOYMENT.md
```

---

# 🔧 Rollback

## Lambda

Use the previous verified published Lambda version.

Do not rely on `$LATEST`.

---

## Glue

Restore the previous immutable S3 application artifact.

---

## Terraform

Use Terraform only for infrastructure rollback.

Application rollback remains with the application deployment workflow.

---

# 💸 Cost Controls

Before processing a large historical release:

```text
Verify worker counts
Verify execution class
Verify timeout
Verify concurrency
Verify S3 paths
Verify expected outputs
```

Recommended progression:

```text
Terraform
    ↓
Controlled runtime test
    ↓
Incremental path verification
    ↓
Failure/recovery testing
    ↓
Historical backfill
```

Do not trigger the full historical backfill just to test infrastructure.

---

# 🧹 Cleanup

Review destruction first:

```powershell
terraform plan -destroy
```

Only if the environment should really be deleted:

```powershell
terraform destroy
```

Review carefully before approval.

---

# 🚫 Operational Rules

Do not:

```text
Commit AWS credentials
Commit Terraform state
Commit .env files
Commit raw production datasets
Use long-lived AWS keys in GitHub Actions
Use terraform -lock=false
Run historical backfills automatically from Terraform
Let Terraform overwrite CI/CD-managed Glue scripts
Use Terraform for Lambda application rollback
Run git add . blindly
Approve unexpected destructive plans
Claim E2E success from infrastructure deployment alone
```

Do:

```text
Review terraform plan
Use remote state
Use GitHub OIDC
Keep artifacts immutable
Verify deployed checksums
Verify Glue ScriptLocations
Verify S3 → SQS
Verify SQS → Lambda
Verify Step Functions
Verify data outputs
Record deployment evidence
Test failure and recovery
```

---

# 📊 Deployment Verification

The deployment phase has been verified across:

```text
Terraform plan                     ✅
Lambda deployment                 ✅
8 Glue job deployments            ✅
S3 → SQS event notification       ✅
SQS → Lambda mapping              ✅
DynamoDB control table            ✅
Step Functions Standard workflow  ✅
IAM / GitHub OIDC                 ✅
Immutable deployment artifacts    ✅
Git working tree                  ✅
```

The project therefore contains:

```text
Infrastructure
+
Data Processing
+
Orchestration
+
Control Plane
+
CI/CD
+
Operational Documentation
+
Architecture Evidence
```

---

# 🧠 What This Project Demonstrates

This project demonstrates practical experience with:

- AWS lakehouse architecture
- Apache Iceberg
- Spark on AWS Glue
- S3 event-driven ingestion
- SQS buffering
- Lambda event handling
- DynamoDB control-plane design
- Step Functions orchestration
- historical backfills
- incremental file processing
- schema evolution
- data-quality validation
- quarantine
- idempotency
- immutable application deployment
- Terraform
- GitHub Actions
- AWS OIDC
- operational debugging
- deployment verification
- failure analysis
- cost-aware architecture

---

# 📚 Documentation

| Resource | Purpose |
|---|---|
| [`README.md`](README.md) | Project overview and reproduction guide |
| [`DEPLOYMENT.md`](DEPLOYMENT.md) | Deployment and operational runbook |
| [`architecture/`](architecture/) | Architecture diagrams |
| [`stepfunctions/`](stepfunctions/) | Workflow definition |
| [`schemas/`](schemas/) | Schema governance |
| [`glue_scripts/`](glue_scripts/) | Glue/Spark applications |
| [`lambda/`](lambda/) | Event-driven Lambda |
| [`terraform/`](terraform/) | Infrastructure as Code |

---

# 🚧 Future Improvements

Planned hardening areas include:

- stale `PROCESSING` state reconciliation
- stronger automated E2E tests
- CloudWatch dashboards
- CloudWatch alarms
- richer SNS notifications
- Lambda aliases for deployment promotion
- dedicated Glue rollback parameters
- Terraform variableization for easier multi-account reuse
- automated deployment smoke tests
- more detailed runtime quality reporting
- additional performance benchmarks
- stronger recovery automation
- automated failure-injection tests

---

# 🤝 Contributing

This repository is primarily a portfolio and engineering learning project.

Constructive improvements are welcome.

Examples:

- architecture improvements
- testing improvements
- security improvements
- cost optimizations
- documentation improvements
- runtime hardening
- CI/CD improvements

Please open an Issue or Pull Request with a clear description.

---

# 📬 Connect

**LinkedIn:**  
https://www.linkedin.com/in/abdul-khadir-44876735a

---

# ⭐ Support

If you find the project useful or interesting, consider giving the repository a ⭐.

---

# 📄 License

A separate open-source `LICENSE` file is not currently included in this repository.

Before accepting external reuse or contributions, add an appropriate open-source license such as MIT or Apache-2.0, depending on the intended usage.

---

# ✅ Final Reproduction Checklist

```text
[ ] Clone repository
[ ] Install Git
[ ] Install AWS CLI
[ ] Install Terraform
[ ] Install Python
[ ] Configure AWS authentication
[ ] Create Terraform state bucket
[ ] Configure backend
[ ] Replace account-specific identifiers
[ ] Configure GitHub OIDC
[ ] Configure deployment IAM role
[ ] terraform init
[ ] terraform validate
[ ] terraform plan
[ ] terraform apply
[ ] terraform plan → No changes
[ ] Deploy Lambda
[ ] Verify Lambda published version
[ ] Deploy Glue
[ ] Verify all 8 Glue jobs
[ ] Verify S3 → SQS
[ ] Verify SQS → Lambda
[ ] Verify Step Functions
[ ] Upload controlled CSV
[ ] Verify SQS event
[ ] Verify Lambda invocation
[ ] Verify DynamoDB registration
[ ] Verify Step Functions execution
[ ] Verify Bronze
[ ] Verify Silver
[ ] Verify DQ
[ ] Verify Gold
[ ] Verify DynamoDB lock release
[ ] Verify SQS empty
[ ] Verify DLQ empty
[ ] Run controlled failure test
[ ] Verify resume/recovery
[ ] Record deployment evidence
```

---

# 🏁 Final Architecture

```text
                              BACKBLAZE
                                  │
                                  ▼
                         ┌─────────────────┐
                         │    Amazon S3    │
                         │      RAW        │
                         └────────┬────────┘
                                  │
                         ObjectCreated
                                  │
                                  ▼
                         ┌─────────────────┐
                         │      SQS        │
                         │     + DLQ       │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │     Lambda      │
                         │ Event Handler   │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │    DynamoDB     │
                         │  Control Plane  │
                         └────────┬────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │   Step Functions        │
                    │       Standard          │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │       AWS Glue          │
                    │     Apache Spark        │
                    └────────────┬────────────┘
                                 │
            ┌────────────────────┼────────────────────┐
            │                    │                    │
            ▼                    ▼                    ▼
      ┌───────────┐       ┌───────────┐       ┌───────────┐
      │  BRONZE   │──────►│  SILVER   │──────►│    DQ     │
      │  Iceberg  │       │  Iceberg  │       │ /Quarantine│
      └───────────┘       └───────────┘       └─────┬─────┘
                                                    │
                                                    ▼
                                              ┌───────────┐
                                              │   GOLD    │
                                              │  Iceberg  │
                                              └─────┬─────┘
                                                    │
                                                    ▼
                                             Athena / BI


INFRASTRUCTURE
──────────────────────────────────────────────────────────────

                         Terraform
                             │
       ┌─────────────┬───────┼─────────┬──────────┐
       ▼             ▼       ▼         ▼          ▼
      S3            SQS     IAM       Glue    Step Functions
                             │
                             ▼
                        GitHub OIDC


APPLICATION DELIVERY
──────────────────────────────────────────────────────────────

                         GitHub
                           │
                           ▼
                    GitHub Actions
                           │
                 ┌─────────┴─────────┐
                 ▼                   ▼
             Lambda CI/CD        Glue CI/CD
                 │                   │
                 ▼                   ▼
          Immutable S3          Immutable S3
             Artifact              Artifact
                 │                   │
                 ▼                   ▼
             Lambda                Glue
```

---

# 🚀 Project Philosophy

The project is not intended to demonstrate only that Spark can transform data.

The goal is to demonstrate how a modern Data Engineering platform can be built around:

```text
Reliable Ingestion
        +
Explicit Control-Plane State
        +
Idempotent Processing
        +
Schema Governance
        +
Data Quality
        +
Transactional Lakehouse Storage
        +
Failure Handling
        +
Infrastructure as Code
        +
CI/CD
        +
Operational Evidence
```

The pipeline is treated as a **system**, not merely a collection of scripts.


