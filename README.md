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

# 2. 🧭 How This Project Works From Start to Finish

This project has **two major phases**:

```text
PHASE 1
Historical Backfill

PHASE 2
Event-Driven Incremental Processing
```

The first phase builds the initial lakehouse.

The second phase keeps that lakehouse updated as new Backblaze data arrives.

The complete idea is:

```text
Historical Data
      ↓
Build the Lakehouse Baseline
      ↓
New Daily Data Arrives
      ↓
Event-Driven Pipeline Automatically Processes It
      ↓
Lakehouse Stays Updated
```

---

# 2.1 📚 Phase 1 — Historical Backfill

The project first needs to build a historical baseline.

The historical scope contains:

```text
228 Backblaze historical files
```

These files already exist, so they are processed as a controlled backfill.

The historical flow is:

```text
228 Historical Files
        ↓
      S3 RAW
        ↓
Full Load Bronze
        ↓
Full Load Silver
        ↓
Data Quality
        ↓
      Gold
```

The full-load jobs used for this phase are:

```text
bronze_ingestion
silver_cleaned
data_quality_check
gold_layer
```

The purpose of this phase is simple:

```text
Take the historical Backblaze data
        ↓
Process it
        ↓
Create the initial lakehouse state
```

This creates the foundation that the ongoing pipeline will maintain.

---

# 2.2 🏗️ Why the Historical Backfill Comes First

The pipeline cannot maintain a lakehouse state that does not exist yet.

Therefore, the project first establishes the historical baseline.

```text
BACKBLAZE HISTORICAL DATA
          ↓
    HISTORICAL BACKFILL
          ↓
   INITIAL LAKEHOUSE STATE
```

After that baseline exists, new files can be added incrementally.

The project therefore follows:

```text
BUILD THE BASELINE
        ↓
MAINTAIN THE BASELINE
```

The historical backfill and ongoing ingestion are therefore two different operating modes.

---

# 2.3 ⚡ Phase 2 — Event-Driven Incremental Processing

Backblaze does not stop providing data after the historical files have been processed.

New CSV data continues to arrive.

The project therefore needs a pipeline that can react automatically whenever a new file arrives.

This is where the **event-driven pipeline** begins.

The ongoing flow is:

```text
New Backblaze CSV
        ↓
S3
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
Incremental Glue Processing
        ↓
Bronze
        ↓
Silver
        ↓
Data Quality
        ↓
Gold
```

The key point is:

```text
A new file arrives
        ↓
The file creates an event
        ↓
The event starts the processing workflow
```

The pipeline does not require an engineer to manually start a Glue job for every new file.

---

# 2.4 🔔 Why This Is Called an Event-Driven Pipeline

The incremental pipeline is called **event-driven** because the arrival of a new file creates the event that starts the control flow.

The trigger is:

```text
S3 ObjectCreated Event
```

The basic idea is:

```text
NO NEW FILE
      ↓
NOTHING TO PROCESS

NEW FILE ARRIVES
      ↓
EVENT CREATED
      ↓
EVENT ENTERS PIPELINE
      ↓
PROCESSING STARTS
```

The pipeline therefore reacts to an event instead of continuously checking whether something has changed.

The event-driven chain is:

```text
S3
 ↓
ObjectCreated Event
 ↓
SQS
 ↓
Lambda
 ↓
Step Functions
 ↓
Glue
```

---

# 2.5 🔀 The Two Phases Together

The complete project lifecycle is:

```text
                 BACKBLAZE
                    │
                    ▼
        ┌────────────────────────┐
        │ HISTORICAL DATA        │
        │ 228 FILE BACKFILL      │
        └───────────┬────────────┘
                    │
                    ▼
                  S3 RAW
                    │
                    ▼
             FULL LOAD PIPELINE
                    │
                    ▼
             Bronze → Silver
                    │
                    ▼
               Data Quality
                    │
                    ▼
                  Gold
                    │
                    ▼
           HISTORICAL BASELINE
                    │
                    │
                    ▼
        ┌────────────────────────┐
        │ NEW DAILY DATA         │
        │ EVENT-DRIVEN MODE      │
        └───────────┬────────────┘
                    │
                    ▼
              New CSV Arrives
                    │
                    ▼
                  S3
                    │
                    ▼
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
             Incremental Glue
                    │
                    ▼
             Bronze → Silver
                    │
                    ▼
               Data Quality
                    │
                    ▼
                  Gold
                    │
                    ▼
          UPDATED LAKEHOUSE
```

This is the overall operating model of the project.

---

# 2.6 🧠 Historical Processing vs Event-Driven Processing

The two phases have different purposes.

| Area | Historical Backfill | Event-Driven Incremental |
|---|---|---|
| Purpose | Build the historical baseline | Keep the lakehouse updated |
| Input | Existing historical files | Newly arriving CSV files |
| Trigger | Controlled backfill execution | S3 object-created event |
| Processing model | Batch / backfill | Event-driven |
| Unit of work | Historical release | Individual source file |
| Glue path | Full-load jobs | Incremental jobs |
| Main objective | Establish state | Maintain state |

The design can therefore be summarized as:

```text
HISTORICAL
→ BUILD STATE

EVENT-DRIVEN INCREMENTAL
→ MAINTAIN STATE
```

---

# 2.7 📦 One New File Becomes One Processing Unit

Once the system is operating in event-driven mode, a newly arriving CSV becomes the processing unit.

For example:

```text
2026-03-31.csv
```

arrives.

The logical flow is:

```text
1 Source File
      ↓
1 S3 ObjectCreated Event
      ↓
1 SQS Message
      ↓
1 File Control Record
      ↓
1 Processing Run
```

The incremental pipeline carries the identity of that file through the control plane.

The downstream processing is therefore based on the exact source file that caused the event.

The incremental jobs receive:

```text
--input_path
--release_id
```

This means the pipeline processes:

```text
THIS FILE
```

rather than:

```text
EVERY FILE IN S3
```

That is an important part of the project's incremental design.

---

# 2.8 🎯 The Main Idea of the Project

The project can be understood in one simple sequence:

```text
FIRST
Process the 228 historical files.

        ↓

THEN
Build the historical Bronze, Silver,
Data Quality and Gold state.

        ↓

THEN
Wait for new Backblaze data.

        ↓

WHEN A NEW CSV ARRIVES
S3 creates an event.

        ↓

THE EVENT ENTERS THE CONTROL PLANE.

        ↓

THE CONTROL PLANE
registers, tracks and orchestrates the work.

        ↓

THE DATA PLANE
processes the actual CSV.

        ↓

THE LAKEHOUSE
is updated.

        ↓

THE SYSTEM
waits for the next file.
```

The complete project lifecycle is therefore:

```text
HISTORICAL BACKFILL
        ↓
LAKEHOUSE BASELINE
        ↓
EVENT-DRIVEN INCREMENTAL PIPELINE
        ↓
NEW FILE ARRIVES
        ↓
PROCESS FILE
        ↓
UPDATE LAKEHOUSE
        ↓
WAIT FOR NEXT FILE
```

This historical-to-event-driven transition is the foundation of the architecture.

---
