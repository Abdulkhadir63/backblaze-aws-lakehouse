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

I built this project around the **Backblaze Drive Stats** dataset.

The dataset contains a large amount of hard-drive statistics collected over time.

The difficult part of this project was not simply reading CSV files with Spark.

The actual problem was building a pipeline that can process a large historical dataset, establish a reliable lakehouse, and then continue processing new files as they arrive.

The pipeline needs to answer practical questions such as:

- How do I load a large historical dataset in controlled scopes?
- How do I process a new CSV automatically when it arrives?
- What happens if the same S3 event is delivered more than once?
- How do I know which file is currently being processed?
- How do I prevent two files from being processed at the same time?
- How do I preserve the original source data?
- How do I handle schema changes across different Backblaze releases?
- What happens when records fail validation?
- How do I move data through Bronze → Silver → Data Quality → Gold?
- What happens when one processing stage fails?
- How can the pipeline resume after a failure?
- How do I keep pipeline state separate from the actual data?
- How do I deploy infrastructure and application code without manually rebuilding everything?

I built the project to handle these problems as one system.

The result is not simply:

```text
CSV → Spark → Table
```

The project has two connected parts:

```text
DATA PLANE
→ Processes the actual data

CONTROL PLANE
→ Controls what should be processed,
  when it should be processed,
  which file is being processed,
  and what happened to that file
```

The **data plane** handles the lakehouse processing:

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

The **control plane** handles the workflow around the data:

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
AWS Glue
```

These two parts have different responsibilities.

The data plane answers:

```text
"How should this data be processed?"
```

The control plane answers:

```text
"What work should happen,
for which file,
when,
and what is the current state of that work?"
```

---

## What I Built

The project starts with a **historical backfill**.

The historical Backblaze data is processed to establish the initial state of the lakehouse:

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

After the historical baseline is established, the project changes into an **event-driven incremental pipeline**.

When a new source CSV arrives, the pipeline reacts to that event automatically:

```text
New CSV File
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
AWS Glue
     ↓
Bronze
     ↓
Silver
     ↓
Data Quality
     ↓
Gold
```

The two processing modes have different purposes:

```text
HISTORICAL
→ Build the initial lakehouse state

INCREMENTAL
→ Maintain that state as new files arrive
```

The current project uses approximately **229 GB of historical data** for the initial backfill.

---

## Technologies Used

I used different AWS and open-source technologies because each one solves a specific problem in the architecture.

| Technology | What I Used It For | Why I Used It |
|---|---|---|
| **Amazon S3** | RAW source storage and event source | Keeps the original source files in durable object storage and generates object-created events |
| **Amazon SQS** | Event buffering | Decouples S3 events from processing and provides a retry boundary and DLQ |
| **AWS Lambda** | Event handling | Validates events, extracts file metadata, registers files, and starts orchestration |
| **Amazon DynamoDB** | Pipeline control state | Tracks files, processing state, and processing ownership |
| **AWS Step Functions** | Workflow orchestration | Controls stage sequencing, claiming, failure paths, and resume logic |
| **AWS Glue** | ETL execution | Runs managed Spark jobs for the data processing stages |
| **Apache Spark** | Distributed processing | Processes large CSV datasets and performs transformations |
| **Apache Iceberg** | Lakehouse table format | Provides managed analytical tables on top of S3 |
| **AWS Glue Data Catalog** | Table/catalog metadata | Provides centralized metadata for Iceberg tables |
| **Amazon Athena** | Query and validation | Queries Iceberg tables and validates processing results |
| **Amazon QuickSight** | Analytical dashboards | Consumes the Gold-layer data for reporting and visualization |
| **Terraform** | Infrastructure as code | Defines AWS infrastructure as version-controlled code |
| **GitHub Actions** | CI/CD | Automates validation and deployment workflows |
| **Amazon SNS** | Notification infrastructure | Provides a path for pipeline alerts and notifications |
| **Python** | Glue, Lambda, and project scripts | Used for ETL logic, event handling, and automation |

---

## Why I Separated the Data Plane and Control Plane

A single Spark pipeline could perform:

```text
Read CSV
   ↓
Transform
   ↓
Write Bronze
   ↓
Write Silver
   ↓
Run DQ
   ↓
Write Gold
```

But that would leave important operational problems unsolved.

For example:

```text
What happens if the same S3 event arrives twice?

What happens if Lambda registers the file
but fails before starting the workflow?

What happens if Bronze succeeds but Silver fails?

How do I know which file currently owns the processing slot?

How do I resume processing after a failure?

Where do I store pipeline state?
```

Instead of putting all of that logic inside the Spark jobs, I separated responsibilities.

```text
CONTROL PLANE
→ manages the work

DATA PLANE
→ processes the data
```

That gives the project this overall structure:

```text
┌─────────────────────────────────────────────┐
│                  CONTROL PLANE              │
│                                             │
│ S3 Event → SQS → Lambda → DynamoDB         │
│                         ↓                   │
│                  Step Functions             │
│                         ↓                   │
│                       Glue                  │
└─────────────────────────┬───────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────┐
│                    DATA PLANE               │
│                                             │
│    S3 RAW → Bronze → Silver → DQ → Gold   │
│                                             │
│          Spark + Apache Iceberg             │
└─────────────────────────────────────────────┘
```

The control plane manages **what should happen**.

The data plane handles **the actual data processing**.

That separation is the foundation of the architecture.
