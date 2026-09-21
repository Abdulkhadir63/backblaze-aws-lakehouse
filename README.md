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
# 2. 🔄 How This Project Actually Works

I built this project in two stages.

First, I had to load the historical Backblaze data and build the initial lakehouse.

After that, the problem changed.

Backblaze keeps providing new CSV data daily data arriving time is not guaranteed, so I needed a way to process a new file automatically whenever it arrives.

That is why this project has two different processing modes:

```text
1. Historical Backfill
2. Event-Driven Incremental Processing
```

The overall idea is:

```text
Historical Backblaze Data
        ↓
   Historical Backfill
        ↓
   Initial Lakehouse
        ↓
New Backblaze CSV Arrives
        ↓
   Event-Driven Pipeline
        ↓
   Process New File
        ↓
   Update Lakehouse
```

I did not want to treat these two problems as exactly the same thing.

Historical data is already there, so I can process it in a controlled way.

New daily data is different because I don't know exactly when the next file will arrive.

The pipeline therefore needs to wait for new data and react when it arrives.

---

## 2.1 📚 First Stage — Historical Backfill

The project first starts with the historical Backblaze data.

The current backfill contains:

```text
4,642  historical files
```

and around:

```text
229 GB of historical data
```

The first job is to get this data into the lakehouse.

The historical flow is:

```text
Historical Backblaze Data
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

The full-load jobs are:

```text
bronze_ingestion
silver_cleaned
data_quality_check
gold_layer
```

At this stage, I am not waiting for an S3 event for every historical file.

The data already exists.

So the job is simply:

```text
Take the historical data
        ↓
Process it
        ↓
Build the lakehouse
```

---

## 2.2 🎯 What Is the Historical Baseline?

The historical baseline is basically the starting point of the lakehouse.

Before the backfill:

```text
Lakehouse
    ↓
Historical data is not loaded yet
```

After the backfill:

```text
Lakehouse
    ↓
Historical data is available
    ↓
Bronze
    ↓
Silver
    ↓
Data Quality
    ↓
Gold
```

So when I say:

```text
"Historical baseline"
```

I mean:

```text
"The lakehouse now contains the historical data
that this project has loaded."
```

This gives the project a known starting point.

After that, I don't want to rebuild the whole historical dataset every time a new CSV arrives.

I only want to process the new data.

---

## 2.3 🧱 Why Historical Processing Is a Backfill

A backfill is simply taking data that already exists and loading it into the system.

In this project:

```text
Historical Data
      ↓
Backfill
      ↓
Lakehouse
```

The historical jobs work around the release structure of the Backblaze data.

The processing is controlled using:

```text
--release_id
```

So the question during historical processing is:

```text
"Which release am I processing?"
```

For example:

```text
Release
    ↓
Bronze
    ↓
Silver
    ↓
DQ
    ↓
Gold
```

This is different from the incremental pipeline.

---

## 2.4 🔀 What Changes After the Historical Backfill?

Once the historical data has been loaded, the problem is no longer:

```text
"How do I load the old data?"
```

Now the problem is:

```text
"How do I process the next file when it arrives?"
```

That is an important change.

The historical phase is:

```text
Known data
    ↓
Controlled backfill
```

The ongoing phase is:

```text
New data
    ↓
Wait for file
    ↓
React to file arrival
```

So the project changes from:

```text
HISTORICAL BACKFILL
```

to:

```text
EVENT-DRIVEN INCREMENTAL PROCESSING
```

The complete transition looks like this:

```text
                HISTORICAL PHASE
                      │
                      ▼
               228 HISTORICAL FILES
                      │
                      ▼
                FULL BACKFILL
                      │
                      ▼
             HISTORICAL BASELINE
                      │
                      ▼
              ────────────────
                 AFTER THAT
              ────────────────
                      │
                      ▼
             NEW DAILY FILES
                      │
                      ▼
            EVENT-DRIVEN PIPELINE
```

This is one of the main design decisions in the project.

---

## 2.5 ⚡ Why This Is an Event-Driven Pipeline

The incremental part of this project is event-driven because the arrival of a new file creates the event that starts the processing flow.

For example:

```text
2026-03-31.csv
```

arrives in the RAW S3 location.

The pipeline does not need me to manually say:

```text
"Start the pipeline now."
```

Instead:

```text
New File Arrives
      ↓
S3 Creates Event
      ↓
Pipeline Reacts
```

That is what I mean when I call this an:

```text
EVENT-DRIVEN PIPELINE
```

The important part is the trigger.

The new file arrival is the trigger.

---

## 2.6 🔔 What Actually Triggers the Pipeline?

The trigger starts at S3.

When a matching CSV file is created in the RAW location:

```text
S3
 ↓
ObjectCreated Event
```

That event is then sent into the control plane:

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

After that, Step Functions starts the actual data-processing workflow:

```text
Step Functions
        ↓
Bronze
        ↓
Silver
        ↓
Data Quality
        ↓
Gold
```

So the full event-driven flow is:

```text
New CSV
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
Glue / Spark
   ↓
Bronze
   ↓
Silver
   ↓
Data Quality
   ↓
Gold
```

That is the core runtime flow of this project.

---

## 2.7 🔍 Event-Driven vs Polling

The easiest way to understand this is to compare it with polling.

A polling system could keep doing this:

```text
Check S3
   ↓
Is there a new file?
   ↓
No
   ↓
Wait
   ↓
Check S3 again
   ↓
Is there a new file?
   ↓
No
   ↓
Wait
```

The system keeps asking whether something happened.

In my project, the idea is different:

```text
Wait
   ↓
New file arrives
   ↓
S3 sends an event
   ↓
Pipeline reacts
```

So:

```text
Polling
→ The pipeline keeps checking for work.

Event-driven
→ The pipeline is notified when work arrives.
```

That is the reason the incremental side of this project is event-driven.

---

## 2.8 📦 One New File Is One Processing Unit

For the incremental pipeline, I treat the source file as the unit of work.

For example:

```text
2026-03-31.csv
```

becomes one processing unit.

The idea is:

```text
1 Source File
      ↓
1 S3 Event
      ↓
1 SQS Message
      ↓
1 File Control Record
      ↓
1 Processing Run
```

This makes the processing much easier to track.

Instead of saying:

```text
"Process whatever is in the bucket."
```

the system can say:

```text
"Process this exact file."
```

That is why the incremental jobs receive:

```text
--input_path
--release_id
```

The `input_path` tells the job exactly which source file it needs to process.

---

## 2.9 🎯 Why I Chose a File-Oriented Incremental Pipeline

Suppose three files arrive:

```text
2026-03-29.csv
2026-03-30.csv
2026-03-31.csv
```

I need to know what happened to each one.

For example:

```text
2026-03-29.csv
→ SUCCESS

2026-03-30.csv
→ FAILED at Data Quality

2026-03-31.csv
→ PENDING
```

That is much easier when each file is a clear processing unit.

The pipeline can answer:

```text
Which file arrived?

Which file is being processed?

Which file failed?

Which processing run belongs to it?

Which stage failed?
```

That is a real operational reason for making the incremental pipeline file-oriented.

---

## 2.10 🧠 Historical and Incremental Pipelines Ask Different Questions

The two pipelines are doing different jobs.

The historical pipeline asks:

```text
"Which release should I process?"
```

The incremental pipeline asks:

```text
"Which new file should I process?"
```

So:

```text
Historical
→ Release-oriented

Incremental
→ File-oriented
```

I kept this distinction in the project because it makes the processing scope much clearer.

---

## 2.11 🏗️ The Two Processing Paths

The historical path is:

```text
Historical Backblaze Data
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

The incremental path is:

```text
New Backblaze CSV
        ↓
      S3 RAW
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

The two paths meet at the same lakehouse.

The difference is how the work starts.

```text
Historical
→ I start the backfill.

Incremental
→ The file arrival starts the workflow.
```

---

## 2.12 🔄 What Happens Every Time a New File Arrives?

After the historical baseline is ready, a new file can arrive.

For example:

```text
2026-03-31.csv
```

The runtime flow is:

```text
1. New CSV arrives
        ↓
2. S3 stores the file
        ↓
3. S3 creates ObjectCreated event
        ↓
4. Event goes to SQS
        ↓
5. Lambda receives the event
        ↓
6. Lambda validates the event
        ↓
7. Lambda extracts the file information
        ↓
8. Lambda registers the file in DynamoDB
        ↓
9. Lambda starts Step Functions
        ↓
10. Step Functions claims the file
        ↓
11. Bronze processes the file
        ↓
12. Silver processes the Bronze data
        ↓
13. Data Quality checks the result
        ↓
14. Gold creates the analytical output
        ↓
15. Step Functions marks the processing successful
        ↓
16. Control state is released
        ↓
17. Pipeline is ready for the next file
```

This is what the pipeline does after the historical backfill.

---

## 2.13 🔐 Why I Did Not Connect S3 Directly to Glue

A very simple pipeline could be:

```text
S3
 ↓
Glue
```

That would work for a basic ETL project.

But this project also needs to deal with:

```text
Duplicate events
Processing state
File ownership
One active processing unit
Failure handling
Recovery
Orchestration
```

Glue is not the right place to manage all of those control-plane concerns.

So I separated the system into two parts.

### Control Plane

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
```

### Data Plane

```text
Glue / Spark
     ↓
  Bronze
     ↓
  Silver
     ↓
    DQ
     ↓
   Gold
```

This separation makes the architecture much easier to reason about.

---

## 2.14 🎛️ What the Control Plane Does

The control plane is mainly responsible for managing the work.

It answers questions like:

```text
Did a new file arrive?

Is this event valid?

Have I already registered this file?

Is another file currently being processed?

Which run owns the current file?

Which stage is running?

Which stage failed?

Can this processing run be resumed?
```

The control plane is:

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

The control plane does not perform the heavy data transformation.

It controls the work.

---

## 2.15 🧱 What the Data Plane Does

The data plane handles the actual data.

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

This is where Spark does the heavy processing.

The data plane is responsible for:

```text
Reading the data
Transforming the data
Applying schema logic
Running data quality checks
Writing lakehouse tables
```

So the simple way I think about the architecture is:

```text
CONTROL PLANE
→ What work should happen?

DATA PLANE
→ How should the data be processed?
```

---

## 2.16 🔁 The Whole Project in One Picture

The complete lifecycle is:

```text
                    BACKBLAZE
                        │
          ┌─────────────┴─────────────┐
          │                           │
          ▼                           ▼
   HISTORICAL DATA              NEW DAILY DATA
          │                           │
          ▼                           ▼
      BACKFILL                  NEW FILE ARRIVES
          │                           │
          ▼                           ▼
        S3 RAW                     S3 RAW
          │                           │
          ▼                           ▼
   Full Load Jobs              ObjectCreated Event
          │                           │
          ▼                           ▼
       Bronze                       SQS
          │                           │
          ▼                           ▼
       Silver                      Lambda
          │                           │
          ▼                           ▼
        DQ                       DynamoDB
          │                           │
          ▼                           ▼
        Gold                   Step Functions
          │                           │
          │                           ▼
          │                         Bronze
          │                           │
          │                           ▼
          │                         Silver
          │                           │
          │                           ▼
          │                          DQ
          │                           │
          │                           ▼
          └──────────────────────── Gold
                                      │
                                      ▼
                              UPDATED LAKEHOUSE
```

The historical side builds the initial state.

The event-driven side keeps updating that state.

---

## 2.17 🔄 How the Lakehouse Keeps Growing

The important thing is that the lakehouse is not a one-time load.

The pattern becomes:

```text
Historical Backfill
       ↓
Historical Baseline
       ↓
New File
       ↓
Process File
       ↓
Update Lakehouse
       ↓
New File
       ↓
Process File
       ↓
Update Lakehouse
       ↓
New File
       ↓
Process File
       ↓
Update Lakehouse
```

That is the ongoing model.

The historical backfill gives me the starting point.

The event-driven pipeline keeps that starting point up to date.

---

## 2.18 🎯 Why This Design Matters

The main point of this architecture is not to add AWS services just for the sake of using them.

Each part has a reason.

```text
S3
→ Stores the source file and generates the event.

SQS
→ Holds the event until it is processed.

Lambda
→ Handles the event and prepares the work.

DynamoDB
→ Keeps the processing state.

Step Functions
→ Controls the processing sequence.

Glue
→ Runs the actual Spark processing.

Iceberg
→ Stores the lakehouse tables.
```

The architecture is basically separating:

```text
"Something new arrived."
```

from:

```text
"Now process that data correctly."
```

S3/SQS/Lambda/DynamoDB/Step Functions handle the first problem.

Glue/Spark/Iceberg handle the second problem.

---

## 2.19 🧠 The Main Idea Behind the Project

The easiest way to understand the whole project is:

```text
First:

Load the historical Backblaze data
and build the lakehouse.

Then:

Stop thinking of the pipeline as
a manual batch job.

Instead:

Wait for new Backblaze files.

When a new file arrives:

Detect it
   ↓
Queue the event
   ↓
Register the file
   ↓
Claim the work
   ↓
Process the file
   ↓
Validate the result
   ↓
Update the lakehouse
```

So the project lifecycle is:

```text
HISTORICAL BACKFILL
        ↓
HISTORICAL BASELINE
        ↓
NEW DAILY FILE
        ↓
EVENT
        ↓
CONTROL
        ↓
PROCESS
        ↓
UPDATE LAKEHOUSE
        ↓
WAIT FOR NEXT FILE
```

That is the basic idea of the project before going deeper into each AWS service.

---

---
