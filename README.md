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

# 2. 🔄 From Historical Backfill to an Event-Driven Pipeline

This Project starts with a historical backfill.

After the historical data has been loaded, the project changes into an event-driven incremental pipeline that processes new Backblaze CSV files as they arrive.

The complete idea is:

```text
HISTORICAL DATA
      ↓
BUILD THE INITIAL LAKEHOUSE
      ↓
HISTORICAL BASELINE COMPLETE
      ↓
NEW DAILY DATA CONTINUES TO ARRIVE
      ↓
EVENT-DRIVEN PIPELINE TAKES OVER
      ↓
PROCESS EACH NEW FILE
      ↓
KEEP THE LAKEHOUSE UPDATED
```

This is the foundation of the entire project.

---

## 2.1 📚 The Project Starts With Historical Data

The first problem is not:

```text
"How do we process tomorrow's file?"
```

The first problem is:

```text
"How do we build the lakehouse from the data that already exists?"
```

Backblaze has historical Drive Stats data that already exists before this pipeline starts processing it.

The project therefore begins with a historical backfill.

The current backfill scope is:

```text
229 GB historical Data
```

These files represent the data that must first be loaded into the lakehouse.

The basic idea is:

```text
229 GB historical Data
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

At this stage, the project is building its initial state.

There is no need to wait for a new event for every historical file.

The files are already available.

The job is to process the historical dataset and establish a usable lakehouse baseline.

---

## 2.2 🎯 What Does "Historical Baseline" Mean?

The phrase **historical baseline** simply means:

```text
"The lakehouse now contains the historical data
that we decided to load."
```

Before the backfill:

```text
Lakehouse
   ↓
No historical baseline
```

After the backfill:

```text
Lakehouse
   ↓
Historical data available
   ↓
Bronze
   ↓
Silver
   ↓
Data Quality
   ↓
Gold
```

This baseline becomes the starting point for everything that happens afterward.

Think of it like taking a starting snapshot of the business data.

The project first gets the lakehouse to a known historical state.

Then it starts maintaining that state.

---

## 2.3 🧱 Historical Processing Is a Backfill Problem

A backfill means:

```text
"Take data that already exists and load it
into the system."
```

The historical flow is therefore controlled.

The project has dedicated full-load jobs:

```text
bronze_ingestion
silver_cleaned
data_quality_check
gold_layer
```

The flow is:

```text
Historical Data
      ↓
Full Load Bronze
      ↓
Full Load Silver
      ↓
Data Quality
      ↓
Full Load Gold
```

The important point is that the historical pipeline is trying to establish the dataset's existing history.

It is not waiting for a future event.

It is processing known data.

---

## 2.4 🗂️ Why the Historical Pipeline Is Release-Oriented

The historical pipeline works around the dataset's release structure.

The processing scope is explicitly controlled using:

```text
--release_id
```

That means the historical processing model is conceptually:

```text
Select Release
      ↓
Process Release
      ↓
Validate Release
      ↓
Publish Release
```

This is different from the later incremental model.

Historical processing asks:

```text
"Which historical release am I processing?"
```

Incremental processing asks:

```text
"Which newly arrived file am I processing?"
```

That difference is extremely important.

---

## 2.5 🔀 The Project Changes Operating Mode After the Backfill

Once the historical baseline is established, the problem changes.

The system now has to deal with new data.

Backblaze continues to produce new CSV data over time.

The pipeline therefore has to answer:

```text
"A new file arrived.
How do we process it automatically?"
```

This is where event-driven processing begins.

The project changes from:

```text
HISTORICAL BACKFILL
```

to:

```text
EVENT-DRIVEN INCREMENTAL PROCESSING
```

The transition is:

```text
                    START
                      │
                      ▼
             Historical Backfill
                      │
                      ▼
             228 Historical Files
                      │
                      ▼
            Historical Baseline
                      │
                      ▼
          ─────────────────────
             NEW DATA ERA
          ─────────────────────
                      │
                      ▼
           New CSV File Arrives
                      │
                      ▼
          Event-Driven Processing
```

The pipeline is now no longer focused on rebuilding history.

It is focused on keeping the lakehouse current.

---

## 2.6 ⚡ What "Event-Driven" Means Here

In this project, **event-driven** means:

```text
The arrival of a new file creates an event,
and that event starts the processing workflow.
```

The system does not need an engineer to manually say:

```text
"Start the pipeline because today's file arrived."
```

Instead:

```text
New File Arrives
      ↓
Event Is Created
      ↓
Pipeline Reacts
```

That is the meaning of event-driven processing in this project.

The file arrival is the trigger.

---

## 2.7 🔔 The New File Becomes the Trigger

Suppose a new file arrives:

```text
2026-03-31.csv
```

The important event is:

```text
"A new source object was created."
```

The pipeline reacts to that event.

Conceptually:

```text
2026-03-31.csv
        ↓
S3
        ↓
ObjectCreated Event
        ↓
Pipeline Wakes Up
```

The pipeline does not need to constantly ask whether `2026-03-31.csv` exists.

The storage system tells the pipeline that the file was created.

That is the fundamental event-driven idea.

---

## 2.8 🔍 Polling vs Event-Driven Processing

It is easier to understand event-driven architecture by comparing it with polling.

### Polling

A polling pipeline might repeatedly do this:

```text
Check S3
   ↓
Is a new file there?
   ↓
No
   ↓
Wait
   ↓
Check S3 again
   ↓
Is a new file there?
   ↓
No
   ↓
Wait
   ↓
Check S3 again
```

The system keeps checking even when nothing has changed.

### Event-Driven

This project follows a different pattern:

```text
Wait
  ↓
New File Arrives
  ↓
S3 Generates Event
  ↓
Pipeline Reacts
```

The difference is:

```text
POLLING
→ Ask whether something happened.

EVENT-DRIVEN
→ Be notified that something happened.
```

That is why this project is described as an **event-driven pipeline**.

---

## 2.9 📦 Each New File Becomes a Processing Unit

The incremental part of the project is designed around the source file.

For example:

```text
2026-03-31.csv
```

becomes one processing unit.

Conceptually:

```text
1 File
  ↓
1 Event
  ↓
1 Queue Message
  ↓
1 File Control Record
  ↓
1 Processing Run
```

This gives the system a clear unit of work.

The pipeline knows:

```text
"This specific file is the work I need to process."
```

That is much more precise than saying:

```text
"Process whatever happens to be in S3."
```

---

## 2.10 🎯 Why File-Oriented Processing Matters

Imagine that three new files arrive:

```text
2026-03-29.csv
2026-03-30.csv
2026-03-31.csv
```

The pipeline should be able to distinguish them.

Instead of treating them as one large unknown workload, the control plane can represent them as individual work items:

```text
FILE A
2026-03-29.csv

FILE B
2026-03-30.csv

FILE C
2026-03-31.csv
```

That makes operational questions much easier to answer.

For example:

```text
Which file arrived?

Which file is being processed?

Which file failed?

Which processing run belongs to that file?

Which file needs recovery?
```

This is one reason the project uses a file-scoped incremental design.

---

## 2.11 🧠 Historical Processing and Incremental Processing Ask Different Questions

The two parts of the project can be understood through two simple questions.

### Historical Pipeline

```text
"Which historical release should I process?"
```

### Incremental Pipeline

```text
"Which newly arrived file should I process?"
```

So the processing models are:

```text
HISTORICAL
→ RELEASE-ORIENTED

INCREMENTAL
→ FILE-ORIENTED
```

This is a deliberate design decision.

---

## 2.12 🔄 What Happens Every Time a New File Arrives?

After the historical baseline has been created, the ongoing runtime looks like this:

```text
New Backblaze CSV
        ↓
S3 receives file
        ↓
S3 creates ObjectCreated event
        ↓
SQS receives event
        ↓
Lambda receives event
        ↓
Lambda validates file
        ↓
Lambda registers file
        ↓
DynamoDB records control state
        ↓
Step Functions starts orchestration
        ↓
File is claimed
        ↓
Bronze runs
        ↓
Silver runs
        ↓
Data Quality runs
        ↓
Gold runs
        ↓
Processing succeeds
        ↓
Pipeline becomes available
        ↓
Ready for the next file
```

This is the ongoing operating cycle of the system.

---

## 2.13 🏗️ Why the Project Needs Two Different Pipelines

It may seem simpler to create only one pipeline and use it for everything.

But the workloads are different.

Historical data is already available:

```text
Known Data
   ↓
Controlled Backfill
```

New daily data is unpredictable from the pipeline's point of view:

```text
Wait
   ↓
New File Arrives
   ↓
React
```

Trying to force both workloads into exactly the same execution model would make the system harder to reason about.

The project therefore uses:

```text
FULL LOAD PATH
→ Historical backfill

INCREMENTAL EVENT-DRIVEN PATH
→ Newly arriving data
```

---

## 2.14 🧱 Full-Load Path

The historical path is:

```text
Historical Source
        ↓
S3 RAW
        ↓
bronze_ingestion
        ↓
silver_cleaned
        ↓
data_quality_check
        ↓
gold_layer
```

This path exists to establish the historical baseline.

It is not the mechanism used to react to every new daily file.

---

## 2.15 ⚡ Incremental Event-Driven Path

The ongoing path is:

```text
New Source File
        ↓
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
bronze_layer
        ↓
silver_layer
        ↓
data_quality_layer
        ↓
gold_analytics_layer
```

This path exists to continuously maintain the lakehouse after the historical baseline has been established.

---

## 2.16 📊 The Two Paths Together

The complete project can therefore be represented as:

```text
                    BACKBLAZE
                        │
            ┌───────────┴───────────┐
            │                       │
            ▼                       ▼
     HISTORICAL DATA          NEW DAILY DATA
            │                       │
            ▼                       ▼
      FULL BACKFILL            S3 OBJECT
            │                       │
            ▼                       ▼
          BRONZE                 EVENT
            │                       │
            ▼                       ▼
          SILVER                  SQS
            │                       │
            ▼                       ▼
           DQ                    Lambda
            │                       │
            ▼                       ▼
          GOLD                 DynamoDB
            │                       │
            │                       ▼
            │                 Step Functions
            │                       │
            │                       ▼
            │                     Bronze
            │                       │
            │                       ▼
            │                     Silver
            │                       │
            │                       ▼
            │                      DQ
            │                       │
            │                       ▼
            └───────────────────── Gold
                                    │
                                    ▼
                           UPDATED LAKEHOUSE
```

The historical path builds the starting state.

The event-driven path maintains that state.

---

## 2.17 🔄 The Lakehouse Changes Over Time

The lakehouse is not supposed to be built once and then forgotten.

The expected lifecycle is:

```text
DAY 1
Historical Backfill
        ↓
Historical Baseline
```

Then:

```text
DAY 2
New File
        ↓
Event-Driven Processing
        ↓
Lakehouse Updated
```

Then:

```text
DAY 3
New File
        ↓
Event-Driven Processing
        ↓
Lakehouse Updated Again
```

Then:

```text
DAY 4
New File
        ↓
Event-Driven Processing
        ↓
Lakehouse Updated Again
```

And so on.

The pattern becomes:

```text
BASELINE
   ↓
NEW FILE
   ↓
UPDATE
   ↓
NEW FILE
   ↓
UPDATE
   ↓
NEW FILE
   ↓
UPDATE
```

This is the long-running operating model of the project.

---

## 2.18 🎯 The Real Purpose of the Event-Driven Design

The purpose is not simply:

```text
"Use more AWS services."
```

The purpose is to solve a real operational requirement:

```text
New data arrives over time.
The pipeline should react automatically.
```

Therefore:

```text
S3
→ Detect the new object

SQS
→ Safely hold the event

Lambda
→ Handle and validate the event

DynamoDB
→ Track processing state

Step Functions
→ Coordinate the workflow

Glue
→ Process the actual data
```

Each service exists because a different part of the lifecycle needs to be handled.

---

## 2.19 🧠 The Most Important Concept in This Project

The most important idea to understand before looking at the AWS services is:

```text
This project is not just a historical ETL pipeline.

It is a pipeline that:

1. Builds a historical baseline
2. Then switches to continuous incremental processing
3. Uses file-arrival events to trigger new work
4. Uses a control plane to manage that work
5. Uses Glue/Spark to process the actual data
```

The lifecycle is:

```text
HISTORICAL BACKFILL
        ↓
HISTORICAL BASELINE
        ↓
NEW DAILY DATA
        ↓
EVENT
        ↓
CONTROL
        ↓
PROCESSING
        ↓
UPDATED LAKEHOUSE
```

That is the foundation of the architecture.

The next section can then explain **exactly how the AWS services connect together to make this event-driven flow work**.
WAIT FOR NEXT FILE
```

This historical-to-event-driven transition is the foundation of the architecture.

---
