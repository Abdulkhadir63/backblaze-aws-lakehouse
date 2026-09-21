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
# **BACKBLAZE AWS MODERN DATA LAKEHOUSE PROJECT**

<p align="center">
  <b>AWS Lakehouse for Backblaze Drive Stats</b><br/>
  Historical Backfill • Event-driven Incremental Processing • Apache Iceberg • AWS Glue • Apache Spark • Lambda • SQS • DynamoDB • Step Functions • Terraform • GitHub Actions
</p>

---

# 1. 📌 What I Built

I built an AWS-based data lakehouse for the Backblaze Drive Stats dataset.

The project handles two different workloads:

1. Historical data that already exists and needs to be loaded into the lakehouse.
2. New source files that arrive later and need to be processed automatically.

Because these are different problems, I built two processing modes.

```text
HISTORICAL BACKFILL

Backblaze Historical Data
        ↓
     Amazon S3
        ↓
      Bronze
        ↓
      Silver
        ↓
  Data Quality
        ↓
       Gold
```

```text
INCREMENTAL PROCESSING

New Backblaze CSV
        ↓
S3 ObjectCreated Event
        ↓
       SQS
        ↓
      Lambda
        ↓
    DynamoDB
        ↓
  Step Functions
        ↓
   AWS Glue + Spark
        ↓
      Bronze
        ↓
      Silver
        ↓
  Data Quality
        ↓
       Gold
```

The historical pipeline establishes the initial state of the lakehouse.

The incremental pipeline maintains that state when new data arrives.

The current historical processing scope is approximately **229 GB of Backblaze data**.

---

# 2. 🏗️  Architecture

I divided the project into two major parts:

```text
DATA PLANE
→ Processes the actual data
```

```text
CONTROL PLANE
→ Controls the processing workflow and keeps track of processing state
```

The data plane is responsible for moving and transforming data.

The control plane is responsible for deciding what should be processed, controlling the workflow, tracking processing state, and handling failures and resume logic.

The complete system is:

<p align="center">
  <img
    src="architecture/project architecuture.svg"
    width="100%"
    alt="Backblaze AWS Lakehouse Architecture"
  />
</p>

The control plane and data plane have different responsibilities, but they work together as one pipeline.

---

# 3. 🔄 Historical Backfill

The first part of the project is the historical backfill.

The Backblaze dataset already contains historical files before the incremental pipeline starts receiving new files.

I therefore built a controlled backfill process to establish the initial lakehouse state.

The historical processing flow is:

```text
Historical Backblaze Data
        ↓
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

The historical processing is release-oriented.

The jobs receive an explicit:

```text
--release_id
```

This means a historical job works on a clearly defined release instead of automatically reading the entire RAW dataset.

The historical Glue jobs are:

```text
bronze_ingestion
silver_cleaned
data_quality_check
gold_layer
```

The responsibilities are separated by stage.

```text
bronze_ingestion
→ Loads historical source data into Bronze

silver_cleaned
→ Cleans and standardizes the Bronze data

data_quality_check
→ Runs validation checks on the processed data

gold_layer
→ Produces analytical Gold data
```

The historical pipeline establishes the initial state of the lakehouse before incremental processing begins.

---

# 4. ⚡ Incremental Event-driven Processing

After the historical baseline is established, the project switches to incremental processing.

The incremental pipeline is event-driven.

A new source file arriving in the RAW S3 location generates an S3 `ObjectCreated` event.

The event then moves through the control plane:

```text
S3
 ↓
ObjectCreated Event
 ↓
SQS
 ↓
Lambda
 ↓
DynamoDB
 ↓
Step Functions
 ↓
AWS Glue
```

The incremental processing jobs are:

```text
bronze_layer
silver_layer
data_quality_layer
gold_analytics_layer
```

The incremental processing model is file-scoped.

Bronze, Silver, and Data Quality receive:

```text
--input_path
--release_id
```

Gold receives:

```text
--release_id
```

The source file is therefore the processing unit for incremental ingestion.

The pipeline does not treat the entire RAW dataset as one processing operation.

---

# 5. 🗂️ Data Plane

The data plane is responsible for processing the actual Backblaze data.

The data path is:

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

The data plane uses:

```text
Amazon S3
AWS Glue
Apache Spark
Apache Iceberg
AWS Glue Data Catalog
Amazon Athena
Amazon QuickSight
```

Each component has a specific responsibility.

| Technology | Purpose |
|---|---|
| Amazon S3 | RAW source storage |
| AWS Glue | Managed Spark execution |
| Apache Spark | Distributed ETL processing |
| Apache Iceberg | Lakehouse table format |
| AWS Glue Data Catalog | Iceberg table catalog |
| Amazon Athena | Querying and validation |
| Amazon QuickSight | Gold-layer analytics |

---

# 6. ☁️ Amazon S3

I use Amazon S3 as the main storage layer.

S3 stores the original Backblaze source files in the RAW zone.

The RAW location is:

```text
raw/drivestats/
```

The project also stores other lakehouse and deployment-related objects in S3:

```text
glue_scripts/
iceberg/
schemas/
reports/
architecture/
dashboard/
```

S3 is responsible for durable object storage.

It is also the event source for incremental processing.

The original source data remains available even when downstream processing fails.

---

# 7. 🧱 Bronze Layer

The Bronze layer is the first lakehouse processing layer.

Its purpose is to ingest source data into the lakehouse while keeping it close to its original form.

Bronze is focused on source ingestion and processing metadata rather than full business transformation.

The historical Bronze job is:

```text
bronze_ingestion
```

The incremental Bronze job is:

```text
bronze_layer
```

The incremental job receives:

```text
--input_path
--release_id
```

The Bronze layer is stored using Apache Iceberg.

---

# 8. 🧹 Silver Layer

The Silver layer is the controlled transformation layer.

Silver is responsible for cleaning and standardizing the source data.

This is also where the project handles differences between source schemas and the canonical structure required by the lakehouse.

The historical Silver job is:

```text
silver_cleaned
```

The incremental Silver job is:

```text
silver_layer
```

The Silver layer is stored using Apache Iceberg.

---

# 9. ✅ Data Quality Layer

The project has a separate Data Quality stage.

Validation logic is not mixed entirely into the transformation jobs.

The historical Data Quality job is:

```text
data_quality_check
```

The incremental Data Quality job is:

```text
data_quality_layer
```

This stage validates the processed data before it reaches Gold.

The project also contains quarantine handling for records that fail validation.

Invalid records can therefore be isolated instead of silently disappearing from the pipeline.

---

# 10. 🏆 Gold Layer

The Gold layer is the analytical layer.

The Gold layer contains data prepared for analytical use rather than raw ingestion.

The historical Gold job is:

```text
gold_layer
```

The incremental Gold job is:

```text
gold_analytics_layer
```

Gold is the output consumed by the analytical layer.

---

# 11. 🧊 Apache Iceberg

I use Apache Iceberg as the table format for the lakehouse.

The project does not treat the lakehouse as a collection of unrelated files.

Iceberg provides the table abstraction over the underlying S3 storage.

The architecture is:

```text
Amazon S3
    +
Apache Iceberg
    +
AWS Glue Data Catalog
    +
Apache Spark
```

The lakehouse tables are organized into:

```text
Bronze
Silver
Gold
```

Iceberg is used to provide managed analytical tables while keeping the underlying storage in S3.

---

# 12. ⚙️ Apache Spark and AWS Glue

I use Apache Spark for distributed data processing.

I run Spark through AWS Glue.

AWS Glue provides the managed execution environment while Spark performs the distributed transformations.

The processing flow is:

```text
AWS Glue
    ↓
Apache Spark
    ↓
Process Data
    ↓
Apache Iceberg
```

The Glue jobs are separated according to the lakehouse stages:

```text
Bronze
Silver
Data Quality
Gold
```

Each stage is therefore independently controlled by the orchestration layer.

---

# 13. 🎛️ Control Plane

The control plane manages the processing workflow.

The control plane is:

```text
S3 ObjectCreated
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

The control plane does not perform the main Spark transformations.

It manages the work around those transformations.

The control plane is responsible for:

```text
Event handling
File registration
Processing state
Processing ownership
Workflow orchestration
Failure handling
Resume handling
Glue execution
```

---

# 14. 📨 Amazon SQS

I use Amazon SQS between S3 and Lambda.

The main queue is:

```text
backblaze-dev-s3-events
```

The project also has a dead-letter queue:

```text
backblaze-dev-s3-events-dlq
```

SQS provides the buffer between the event producer and the event consumer.

This separates S3 event generation from Lambda processing.

SQS also provides a retry boundary.

Messages that cannot be processed successfully after the configured retry behavior can move to the DLQ.

---

# 15. 🔧 AWS Lambda

I use Lambda as the S3 event handler.

The Lambda function is:

```text
backblaze-dev-s3-event-handler
```

Lambda does not execute Spark.

Its responsibility is control-plane processing.

The Lambda:

```text
Receives the SQS event
        ↓
Reads the S3 event
        ↓
Validates the bucket and RAW prefix
        ↓
Decodes the S3 object key
        ↓
Extracts release and source information
        ↓
Captures file metadata
        ↓
Registers the source file in DynamoDB
        ↓
Starts Step Functions
```

The Lambda captures metadata such as:

```text
source_file
size
etag
event_name
event_time
received_at
release_id
```

The file registration is designed to be duplicate-safe.

Lambda does not own the entire workflow.

Its responsibility is event handling and workflow initiation.

---

# 16. 🗄️ Amazon DynamoDB

I use DynamoDB as the control-plane state store.

The table is:

```text
backblaze-dev-pipeline-control
```

The project also uses the processing queue index:

```text
backblaze-processing-queue-index
```

DynamoDB stores pipeline state rather than analytical data.

The lakehouse tables answer:

```text
"What data has been processed?"
```

DynamoDB answers:

```text
"What processing work exists?"
"What is its current state?"
"Which file is being processed?"
"Can the file be claimed?"
"Where should processing resume?"
```

This keeps operational pipeline state separate from the lakehouse data.

The file registration uses conditional logic so duplicate event delivery does not automatically create duplicate processing records.

---

# 17. 🔀 AWS Step Functions

I use AWS Step Functions as the orchestration layer.

The state machine is:

```text
backblaze-dev-file-processing
```

Step Functions controls the sequence of processing stages.

The normal processing path is:

```text
Claim Processing Unit
        ↓
Run Bronze
        ↓
Run Silver
        ↓
Run Data Quality
        ↓
Run Gold
        ↓
Mark Processing Successful
        ↓
Release Processing State
```

The state machine also contains failure and resume paths.

The project is designed around one active processing unit at a time.

For the incremental pipeline, that processing unit is a source file.

Step Functions coordinates the work rather than performing the transformations itself.

---

# 18. 🔒 Processing Ownership

The project uses DynamoDB and Step Functions together to control processing ownership.

The pipeline maintains processing state so the workflow can determine whether processing is already active.

The processing model is:

```text
One active processing unit
        ↓
Process that unit
        ↓
Complete or fail
        ↓
Release processing state
        ↓
Allow the next unit to run
```

This control state is separate from the data stored in Bronze, Silver, and Gold.

The control plane therefore knows which processing unit currently owns the processing slot.

---

# 19. 🔁 Failure and Resume Handling

Failure handling is implemented in the orchestration layer.

The state machine keeps track of the processing stage.

The workflow distinguishes between:

```text
A new processing request
```

and:

```text
A processing unit that already exists
but previously failed
```

The Step Functions workflow checks existing pipeline state and failed-file state before deciding which stage should run.

This allows the pipeline to resume processing instead of treating every retry as an entirely new piece of work.

The processing stages remain separate:

```text
Bronze
Silver
Data Quality
Gold
```

The orchestration layer can therefore determine where processing needs to continue.

---

# 20. 🔍 Amazon Athena

I use Amazon Athena to query the Iceberg tables.

Athena is used for:

```text
Data validation
Row-count verification
Schema verification
Release validation
Data Quality inspection
Gold-layer analysis
```

Athena provides a query interface over the lakehouse tables without requiring a separate database server.

---

# 21. 📊 Amazon QuickSight

I use Amazon QuickSight as the analytical consumption layer.

The dashboard consumes processed analytical data from the Gold layer.

The overall path is:

```text
Backblaze
   ↓
S3 RAW
   ↓
Bronze
   ↓
Silver
   ↓
Data Quality
   ↓
Gold
   ↓
Athena
   ↓
QuickSight
```

The dashboard is therefore separated from the ingestion and transformation stages.

---

# 22. 🔔 Amazon SNS

I created an SNS notification layer for pipeline notifications.

The SNS topic is:

```text
backblaze-dev-pipeline-notifications
```

SNS provides the notification infrastructure for pipeline alerts.

At the current recorded project state, the SNS infrastructure exists, but notification states had not yet been added directly into the Step Functions state machine.

---

# 23. 🏗️ Terraform

I use Terraform for infrastructure as code.

Terraform defines and manages AWS infrastructure instead of making the environment dependent on manual console configuration.

The project infrastructure includes:

```text
Amazon S3
Amazon SQS
SQS DLQ
AWS Lambda
Amazon DynamoDB
AWS Step Functions
AWS IAM
Amazon SNS
```

The purpose of Terraform is to keep infrastructure configuration in source control and make the environment reproducible.

---

# 24. 🚀 GitHub Actions

I use GitHub Actions for CI/CD.

The repository contains the application and infrastructure code.

GitHub Actions automates deployment-related work instead of requiring manual uploads and configuration through the AWS console.

The deployment flow is:

```text
GitHub
   ↓
GitHub Actions
   ↓
Build / Validate / Deploy
   ↓
AWS
```

Terraform manages infrastructure configuration while CI/CD handles the deployment workflow.

---

# 25. 🧱 Complete Architecture

```text
                         BACKBLAZE
                             │
                             ▼
                     ┌─────────────┐
                     │   S3 RAW    │
                     └──────┬──────┘
                            │
                    ObjectCreated
                            │
                            ▼
                     ┌─────────────┐
                     │     SQS     │
                     └──────┬──────┘
                            │
                            ▼
                     ┌─────────────┐
                     │   Lambda    │
                     └──────┬──────┘
                            │
                            ▼
                     ┌─────────────┐
                     │  DynamoDB   │
                     │ Control     │
                     │    State    │
                     └──────┬──────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │   Step Functions   │
                  │    Orchestrator    │
                  └─────────┬──────────┘
                            │
                            ▼
                     ┌─────────────┐
                     │ AWS Glue +  │
                     │ Apache Spark│
                     └──────┬──────┘
                            │
             ┌──────────────┼──────────────┐
             │              │              │
             ▼              ▼              ▼
          Bronze         Silver       Data Quality
             │              │              │
             └──────────────┼──────────────┘
                            │
                            ▼
                          Gold
                            │
                       ┌────┴────┐
                       │         │
                       ▼         ▼
                    Athena   QuickSight
```

## Control Plane

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
 ↓
Glue
```

## Data Plane

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

The control plane manages the lifecycle of the processing unit.

The data plane performs the actual data engineering work.
