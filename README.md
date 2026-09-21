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

# 2. 🏗️ How This Project Works

The pipeline is divided into two parts:

```text
DATA PLANE
→ Processes the actual data

CONTROL PLANE
→ Controls when and how that processing happens
```

The data plane is:

```text
S3 RAW
   ↓
Bronze
   ↓
Silver
   ↓
Data Quality
   ↓
Gold
```

The control plane is:

```text
S3 Event
   ↓
SQS
   ↓
Lambda
   ↓
DynamoDB
   ↓
Step Functions
   ↓
Glue
```

The two parts work together.

The control plane decides **what should be processed**.

The data plane decides **how the data should be processed**.

---

# 3. 🏗️ Project Architecture

The complete architecture is represented in the repository as an SVG diagram.

<p align="center">
  <img
    src="architecture/project%20architecture.svg"
    width="100%"
    alt="Backblaze AWS Lakehouse Architecture"
  />
</p>

The architecture can be understood as:

```text
                         BACKBLAZE DATA
                              │
                              ▼
                    ┌───────────────────┐
                    │    Amazon S3      │
                    │       RAW         │
                    └─────────┬─────────┘
                              │
                     ObjectCreated
                              │
                              ▼
                    ┌───────────────────┐
                    │      Amazon       │
                    │       SQS         │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │      Lambda       │
                    │  Event Handler    │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │    DynamoDB       │
                    │  Control Plane    │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │  Step Functions   │
                    │     Standard      │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │    AWS Glue       │
                    │      Spark        │
                    └─────────┬─────────┘
                              │
                ┌─────────────┼─────────────┐
                │             │             │
                ▼             ▼             ▼
             Bronze        Silver          DQ
                │             │             │
                └─────────────┴─────────────┘
                              │
                              ▼
                            Gold
                              │
                              ▼
                        Analytics
```

The important part is that every service has a specific responsibility.

---

# 4. 🔄 What Happens When a New File Arrives?

To understand the pipeline, imagine a new file arrives:

```text
2026-03-31.csv
```

The complete sequence is:

```text
1. S3 receives the CSV
        ↓
2. S3 creates an ObjectCreated event
        ↓
3. SQS receives the event
        ↓
4. Lambda receives the SQS message
        ↓
5. Lambda validates the event
        ↓
6. Lambda extracts file metadata
        ↓
7. Lambda registers the file in DynamoDB
        ↓
8. Lambda starts Step Functions
        ↓
9. Step Functions finds and claims the file
        ↓
10. Glue runs Bronze
        ↓
11. Glue runs Silver
        ↓
12. Glue runs Data Quality
        ↓
13. Glue runs Gold
        ↓
14. Step Functions records success
        ↓
15. DynamoDB releases the processing lock
```

The following sections explain each step in plain English.

---

# 5. ☁️ Step 1 — S3 Receives the Source File

The first thing that happens is the source CSV is uploaded to the RAW S3 location.

Example:

```text
s3://<raw-bucket>/raw/drivestats/data_Q1_2026/2026-03-31.csv
```

S3 is the first durable location for the source data.

At this point, the pipeline is not performing transformations.

S3 is only responsible for storing the source object.

This is important because the original file must remain available even if a later processing stage fails.

For example:

```text
Source File
     ↓
S3 RAW
     ↓
Processing fails
     ↓
Source file still exists
```

That gives the pipeline a stable source from which the file can be reprocessed.

### What S3 does

```text
Receive file
    ↓
Store file
    ↓
Generate event
```

### What S3 does not do

```text
❌ Transform the CSV
❌ Run Spark
❌ Run Glue
❌ Build Silver tables
❌ Build Gold tables
```

S3 is the storage and event source.

---

# 6. 🔔 Step 2 — S3 Generates an ObjectCreated Event

The S3 bucket is configured with an event notification.

The notification is limited to the RAW CSV location.

The expected filter is:

```text
Prefix:
raw/drivestats/

Suffix:
.csv
```

When a matching file is created, S3 generates an event.

Conceptually:

```text
S3
 │
 │ "A new CSV object was created"
 ▼
SQS
```

The pipeline therefore does not need to continuously scan S3.

Instead of:

```text
"Did another file arrive?"
"Did another file arrive?"
"Did another file arrive?"
```

the system becomes event-driven:

```text
"A file arrived."
```

That event becomes the input to the control plane.

---

# 7. 📨 Step 3 — SQS Receives the Event

The event is sent to:

```text
backblaze-dev-s3-events
```

SQS exists between S3 and Lambda.

Its purpose is to act as a buffer.

The flow is:

```text
S3
 ↓
SQS
 ↓
Lambda
```

This means S3 does not need to directly wait for Lambda to finish.

SQS keeps the event until the consumer successfully processes it.

---

## Why SQS is used

SQS provides:

```text
Buffering
Retry behavior
Asynchronous processing
Dead-letter handling
Burst protection
Decoupling
```

For example, if multiple files arrive:

```text
File A ─┐
File B ─┼──► SQS
File C ─┘
```

Lambda can consume them as processing capacity becomes available.

---

## Dead Letter Queue

The project also has:

```text
backblaze-dev-s3-events-dlq
```

The DLQ exists for events that cannot be processed successfully after the configured retry attempts.

Instead of:

```text
retry forever
```

the event can eventually move to:

```text
Main Queue
      ↓
Retry
      ↓
Retry
      ↓
Retry
      ↓
DLQ
```

This prevents permanently failing events from blocking normal processing.

---

## SQS Batch Size

The SQS → Lambda mapping uses:

```text
BatchSize = 1
```

This is deliberate.

The pipeline uses a file-oriented processing model:

```text
1 Object
   ↓
1 Event
   ↓
1 File Processing Unit
```

That keeps the control-plane state easy to understand and makes troubleshooting easier.

---

# 8. ⚡ Step 4 — Lambda Receives the SQS Message

The Lambda function is:

```text
backblaze-dev-s3-event-handler
```

Lambda is the bridge between:

```text
event handling
```

and:

```text
pipeline orchestration
```

Lambda does not perform the actual large-data processing.

It should not be responsible for reading a 100+ MB CSV and transforming millions of records.

Instead:

```text
Lambda
→ lightweight control-plane work

Glue / Spark
→ heavy data processing
```

This separation is intentional.

---

# 9. 🔍 Step 5 — Lambda Parses the Event

The Lambda receives an SQS event.

Inside the SQS message is the S3 event generated by the object creation.

Lambda extracts information such as:

```text
Bucket
Object Key
Object Size
ETag
Event Name
Event Time
```

From the object path it also determines information required by the pipeline, including:

```text
release_id
source file
source date
```

The Lambda therefore converts:

```text
raw SQS message
```

into:

```text
validated processing information
```

---

# 10. 🛡️ Step 6 — Lambda Validates the Incoming File Event

The event is external input.

The pipeline therefore validates it before allowing it into the control plane.

Lambda checks that the event belongs to the expected environment and expected data path.

Conceptually:

```text
SQS Event
    ↓
Parse Event
    ↓
Check Bucket
    ↓
Check Prefix
    ↓
Check Object
    ↓
Extract Metadata
```

If the event is not valid, it should not create a processing unit.

For example, an unexpected S3 bucket should not be allowed to start the Backblaze pipeline.

This validation keeps invalid events away from the orchestration layer.

---

# 11. 🆔 Step 7 — Lambda Creates the File Control Record

After validation, Lambda registers the file in DynamoDB.

The control table is:

```text
backblaze-dev-pipeline-control
```

A file is represented using an identity similar to:

```text
FILE#s3://.../raw/drivestats/...
```

The important idea is that the file gets a persistent control-plane identity.

The pipeline can now answer:

```text
Which file is this?

What is its status?

Which processing run owns it?

When did processing start?

```

Without this state, the pipeline would have to infer processing status from external systems.

---

# 12. 🔐 Step 8 — DynamoDB Provides Idempotency

S3 and SQS are distributed event systems.

The pipeline therefore cannot rely on the assumption:

```text
One event will always be delivered exactly once.
```

Instead, the file registration is duplicate-safe.

The intended behavior is:

```text
First event
    ↓
FILE record does not exist
    ↓
Create FILE record
```

For a duplicate event:

```text
Duplicate event
    ↓
FILE record already exists
    ↓
Do not blindly create another processing unit
```

This protects the pipeline from turning duplicate event delivery into duplicate processing.

Without this protection, the same file could result in:

```text
1 file
 ↓
2 events
 ↓
2 executions
 ↓
2 Glue processing runs
```

The control plane exists to prevent that.

---

# 13. 🎛️ Step 9 — DynamoDB Tracks the Pipeline Lock

In addition to the file record, the pipeline maintains a control record:

```text
PIPELINE#BACKBLAZE
```

This represents the overall state of the pipeline.

The intended state looks like:

```text
IDLE
  │
  ▼
PROCESSING
  │
  ▼
SUCCESS
  │
  ▼
IDLE
```

During processing, the pipeline records information such as:

```text
processing_run_id
active_source_file
processing_started_at
status
```

This gives the orchestration layer an explicit answer to:

```text
Is the pipeline currently busy?
Which file is active?
Which run owns it?
```

---

# 14. ▶️ Step 10 — Lambda Starts Step Functions

Once Lambda has successfully validated and registered the file, Lambda starts the Step Functions workflow.

The Lambda does not start:

```text
Bronze
Silver
DQ
Gold
```

individually.

Instead:

```text
Lambda
   ↓
Step Functions
   ↓
Glue stages
```

Step Functions becomes responsible for the complete processing sequence.

This prevents orchestration logic from being spread across Lambda code.

---

# 15. 🧠 Step 11 — Step Functions Determines the Processing Work

The Step Functions state machine is:

```text
backblaze-dev-file-processing
```

It is a Standard workflow.

The workflow determines what processing work needs to happen.

Conceptually:

```text
Start
  ↓
Determine Mode
  ↓
Find Pending File
  ↓
Claim File
```

The important point is that Step Functions operates using the control-plane state rather than simply launching Glue blindly.

---

# 16. 🔒 Step 12 — Step Functions Claims the File

Before heavy processing starts, the file must be claimed.

The claim establishes:

```text
This processing run owns this file.
```

The control information includes values such as:

```text
processing_run_id
active_source_file
status
processing_started_at
```

This creates the relationship:

```text
Pipeline
   ↓
Processing Run
   ↓
Source File
```

That relationship becomes important when debugging or recovering from a failure.

---

# 17. 🥉 Step 13 — Step Functions Starts the Bronze Job

Once the file has been claimed, Step Functions starts the incremental Bronze Glue job:

```text
bronze_layer
```

The job receives:

```text
--input_path
--release_id
```

The important design decision here is that Bronze receives the exact file that triggered the processing.

It is not told:

```text
"Process every file in S3."
```

It is told:

```text
"Process this source file."
```

This makes the incremental pipeline deterministic and file-scoped.

---

# 18. 🧹 Step 14 — Bronze Reads the Source File

The Bronze Glue job uses Spark to read the source CSV from S3.

Conceptually:

```text
S3 RAW CSV
     ↓
AWS Glue
     ↓
Apache Spark
```

The Bronze stage is designed to keep the source representation as close to the input as practical.

The goal is to establish a durable lakehouse representation before applying canonical business logic.

---

# 19. 🧱 Step 15 — Bronze Writes the Source-Oriented Dataset

After reading the file, Bronze writes the processed source data into the Bronze Iceberg layer.

The Bronze stage can also attach the operational metadata required by the pipeline.

Typical metadata includes:

```text
release_id
ingested_at
processing_run_id
```

The resulting relationship becomes:

```text
RAW source
    ↓
Bronze Iceberg
```

The important principle is:

```text
Bronze preserves.

Silver transforms.
```

Bronze should not become the location where all business rules are mixed together.

---

# 20. ➡️ Step 16 — Step Functions Moves to Silver

If Bronze succeeds, Step Functions moves to:

```text
silver_layer
```

The Silver job also receives:

```text
--input_path
--release_id
```

The purpose now changes.

Bronze established the durable source-oriented dataset.

Silver makes that dataset consistent and usable.

---

# 21. 🧬 Step 17 — Silver Applies the Canonical Schema

Silver is where the project applies controlled schema logic.

The Silver stage can perform:

```text
Column mapping
Type casting
Canonical field creation
Required-field handling
Deduplication
Schema evolution
```

Conceptually:

```text
Bronze
   ↓
Read Source Structure
   ↓
Map to Canonical Structure
   ↓
Cast Data Types
   ↓
Apply Required Rules
   ↓
Deduplicate
   ↓
Write Silver
```

The reason for putting these rules in Silver is that source schemas can change.

Silver acts as the compatibility boundary between:

```text
changing source data
```

and:

```text
stable downstream data
```

---

# 22. 🧬 Step 18 — Silver Handles Historical Schema Differences

Backblaze releases are not necessarily identical.

A newer release can contain columns that an older release did not contain.

The pipeline therefore separates:

```text
Source Schema
       ↓
Bronze
       ↓
Canonical Schema
       ↓
Silver
```

This means the source layer can preserve new columns without forcing the Bronze layer to understand every business-level schema decision.

Silver decides how those fields should appear in the canonical representation.

Schema definitions are maintained under:

```text
schemas/
```

---

# 23. 🧹 Step 19 — Silver Deduplicates and Validates Required Fields

Silver is also responsible for preventing obvious source-level problems from flowing further downstream.

Examples include:

```text
Missing required field
Invalid value
Duplicate record
Incorrect type
Schema mismatch
```

The result should be a controlled Silver dataset rather than simply a copy of Bronze.

---

# 24. 🧪 Step 20 — Step Functions Starts Data Quality

After Silver completes, Step Functions starts:

```text
data_quality_layer
```

This stage exists because:

```text
Spark job succeeded
```

does not automatically mean:

```text
Data is correct.
```

A Spark job can successfully execute while still producing data that violates business or technical expectations.

The DQ stage therefore creates an explicit acceptance checkpoint.

---

# 25. 🚦 Step 21 — Data Quality Evaluates the Silver Dataset

The DQ stage evaluates the transformed Silver data.

Conceptually:

```text
Silver
   ↓
Data Quality Rules
   │
   ├── Valid
   │
   └── Invalid
```

Invalid records are handled through the project's quarantine / DQ logic.

The important behavior is:

```text
Bad data
   ↓
Explicit decision
```

rather than:

```text
Bad data
   ↓
Silently published to Gold
```

---

# 26. 🥇 Step 22 — Step Functions Starts Gold

Once Data Quality completes successfully, Step Functions starts:

```text
gold_analytics_layer
```

The Gold job receives:

```text
--release_id
```

The Gold stage uses the validated upstream data to create the analytical output.

The overall data progression is:

```text
RAW
 ↓
Bronze
 ↓
Silver
 ↓
DQ
 ↓
Gold
```

Gold is therefore the first layer intended for downstream analytical consumption.

---

# 27. 📊 Step 23 — Gold Creates Analytical Data

Gold is responsible for producing the final analytical representation.

At this point:

```text
Source-specific cleanup
```

and:

```text
Canonical schema handling
```

have already happened upstream.

Gold can therefore focus on analytical structures rather than raw source irregularities.

The goal is to make downstream querying simpler and more predictable.

---

# 28. ✅ Step 24 — Successful Completion

After Gold succeeds, Step Functions executes the success path.

The pipeline records successful completion and releases the processing state.

Conceptually:

```text
PIPELINE
   │
   ▼
PROCESSING
   │
   ▼
SUCCESS
   │
   ▼
IDLE
```

The important result is:

```text
The file completed
+
The pipeline lock was released
```

This allows another processing unit to be handled.

---

# 29. ❌ What Happens When a Glue Stage Fails?

Suppose the sequence is:

```text
Bronze  ✅
Silver  ✅
DQ      ❌
Gold    not started
```

Step Functions should move into its failure handling path.

The pipeline should retain enough control-plane information to determine:

```text
Which file failed?
Which run failed?
Which stage failed?
```

This is more useful than simply seeing:

```text
Glue Job Failed
```

because recovery requires knowing what failed and where.

---

# 30. 🔄 Resume Processing

The Step Functions workflow contains resume logic.

The idea is:

```text
Failed Processing Unit
        ↓
Read Failure State
        ↓
Determine Failed Stage
        ↓
Resume Appropriate Stage
        ↓
Run Remaining Downstream Stages
```

For example:

```text
Bronze ✅
Silver ✅
DQ ❌
Gold not started
```

The recovery path should identify DQ as the failed stage instead of unnecessarily rebuilding the entire pipeline from Bronze.

---

# 31. 🧯 Runtime Failure We Actually Found

During testing, an execution was externally aborted while a Glue stage was running.

The observed state became:

```text
Step Functions
    ↓
ABORTED

Running Executions
    ↓
0

SQS
    ↓
Empty

DynamoDB
    ↓
PROCESSING
```

This exposed an important recovery issue.

When a Step Functions execution is externally aborted, the normal state-machine failure path may not run.

That means the cleanup that normally changes the DynamoDB state may never execute.

The result is:

```text
No active execution
        +
DynamoDB still says PROCESSING
```

This is a runtime reconciliation issue.

It is not the same thing as an infrastructure deployment failure.

The deployment itself was separately validated with Terraform.

---

# 32. 🏛️ Why the Runtime Architecture Uses These Services Together

Each service solves a different problem.

```text
Amazon S3
→ Stores source data and produces the object event

Amazon SQS
→ Buffers and retries the event

AWS Lambda
→ Validates the event and registers processing

Amazon DynamoDB
→ Tracks state and prevents duplicate work

AWS Step Functions
→ Coordinates the processing stages

AWS Glue
→ Performs distributed Spark processing

Apache Iceberg
→ Stores the lakehouse tables
```

The services are therefore not interchangeable.

For example:

```text
Lambda is not Glue.
DynamoDB is not S3.
Step Functions is not Spark.
SQS is not the control database.
```

Each one has a specific job in the system.

---

# 33. 🔀 Historical Processing and Incremental Processing

The project intentionally has two processing modes.

## Historical Processing

Historical data is processed using the full-load jobs:

```text
bronze_ingestion
silver_cleaned
data_quality_check
gold_layer
```

The historical path is release-oriented.

Example:

```text
--release_id data_Q1_2026
```

The objective is to establish the historical lakehouse baseline.

---

## Incremental Processing

Once the baseline exists, newly arriving files use:

```text
bronze_layer
silver_layer
data_quality_layer
gold_analytics_layer
```

The incremental path is file-oriented.

The file that arrives becomes the processing unit.

This is an important difference:

```text
Historical
→ Release-oriented

Incremental
→ File-oriented
```

---

# 34. 🧠 The Complete Pipeline in Plain English

A simple way to understand the project is:

```text
A file arrives.
        ↓
S3 stores it.
        ↓
S3 tells the system that it arrived.
        ↓
SQS safely holds that event.
        ↓
Lambda reads the event.
        ↓
Lambda checks that the event is valid.
        ↓
Lambda records the file in DynamoDB.
        ↓
DynamoDB protects the pipeline from duplicate registration.
        ↓
Lambda starts Step Functions.
        ↓
Step Functions claims the file.
        ↓
Glue reads the file and creates Bronze.
        ↓
Glue converts Bronze into the canonical Silver structure.
        ↓
DQ checks whether the Silver result is acceptable.
        ↓
Gold creates the analytical output.
        ↓
Step Functions records success.
        ↓
DynamoDB releases the pipeline lock.
```

That is the actual runtime story of this project.

---

# 35. 🎯 Why This Is More Than a Spark ETL Project

The project is not just:

```text
Read CSV
   ↓
Transform
   ↓
Write Table
```

It also handles:

```text
Event detection
      +
Event buffering
      +
Idempotent registration
      +
Pipeline state
      +
Orchestration
      +
Schema evolution
      +
Data quality
      +
Failure handling
      +
Recovery
      +
Infrastructure deployment
      +
Application deployment
```

That is the reason the architecture uses multiple AWS services.

Each service addresses a specific operational requirement in the overall pipeline.

---

# 36. 🔗 Deployment and Runtime Are Separate

The project deliberately separates:

```text
DEPLOYMENT
```

from:

```text
RUNTIME
```

Deployment answers:

```text
Does AWS match the Terraform configuration?
Are Lambda and Glue code deployed?
Are S3 → SQS and SQS → Lambda connected?
Is Step Functions deployed?
```

Runtime answers:

```text
Can a real file successfully travel through the complete pipeline?
Can the system recover from failure?
Does the control plane return to IDLE?
Are the expected data outputs created?
```

This distinction is important when evaluating the system.

A successful:

```text
terraform plan
```

does not by itself prove:

```text
S3 → SQS → Lambda → Step Functions → Glue → Gold
```

worked correctly.

---

# 37. 🧭 Project Execution Model

The complete system can therefore be viewed as three stages.

## Stage 1 — Landing

```text
Backblaze
   ↓
S3 RAW
```

The source file is safely stored.

## Stage 2 — Control

```text
S3
 ↓
SQS
 ↓
Lambda
 ↓
DynamoDB
 ↓
Step Functions
```

The system determines whether and how the file should be processed.

## Stage 3 — Processing

```text
Glue
 ↓
Bronze
 ↓
Silver
 ↓
DQ
 ↓
Gold
```

The actual dataset processing happens here.

---

# 38. 📌 Core Design Principle

The most important idea behind the project is:

```text
A data pipeline is not only a transformation problem.

It is also a state,
orchestration,
failure,
deployment,
and recovery problem.
```

That is why the project contains both a data plane and a control plane.

```text
DATA PLANE
→ Process data correctly

CONTROL PLANE
→ Process work correctly
```
