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

# 2. 🏗️ Overall Architecture

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

```text
                    CONTROL PLANE
┌─────────────────────────────────────────────────────────┐
│                                                         │
│  S3 Event → SQS → Lambda → DynamoDB → Step Functions  │
│                                              ↓          │
│                                            Glue         │
│                                                         │
└───────────────────────────────────────────┬─────────────┘
                                            │
                                            ↓
                    DATA PLANE
┌─────────────────────────────────────────────────────────┐
│                                                         │
│        S3 RAW → Bronze → Silver → DQ → Gold            │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

The control plane and data plane have different responsibilities, but they work together as one pipeline.

---

# 3. 🔄 Historical Backfill

The first major part of this project is the historical backfill.

Before the event-driven pipeline can handle new files, I first need to bring the existing Backblaze history into the lakehouse.

For this project, the historical scope covers the Backblaze data from:

```text
2013
   ↓
2014
   ↓
2015
   ↓
2016_Q1
   ↓
...
   ↓
2026_Q1
```

The backfill scope I worked with is approximately:

```text
4,642  files
~229 GB of source data
```

This gives the project a large historical dataset to build the initial lakehouse state from.

---

## 3.1 📚 Why a Historical Backfill Is Needed

The new-file pipeline only makes sense after the existing history is available.

I therefore separated the problem into two parts:

```text
Historical Data
→ Build the initial lakehouse

New Daily Data
→ Keep the lakehouse updated
```

Without the backfill, the incremental pipeline would only know about files arriving from the point it was started.

The backfill gives the lakehouse the historical baseline first.

---

## 3.2 📦 Historical Data Scope

The Backblaze source is released over multiple years.

The project starts with the older 2013 data and continues through the later quarterly releases, up to the current historical scope used in this project:

```text
2013 → 2015
Annual releases

2016 onward
Quarterly releases

Project historical scope
2013 → 2026_Q1
```

Instead of treating roughly 229 GB as one undifferentiated input, I process the history using explicit release-level scope.

```text
2013
2014
2015
2016_Q1
2016_Q2
...
2026_Q1
```

This gives each historical processing run a clear boundary.

---

## 3.3 🎯 How the Backfill Is Controlled

The historical jobs use:

```text
--release_id
```

So a job is told exactly which release it is supposed to process.

For example:

```text
--release_id 2013
```

or:

```text
--release_id 2016_Q1
```

The important part is that the Glue job does not simply read the entire RAW dataset.

The processing scope is explicit:

```text
Selected Release
      ↓
Read only that release
      ↓
Process it
      ↓
Write the result
```

This keeps the historical pipeline controlled and makes a rerun much easier to reason about.

---

## 3.4 🏗️ Historical Processing Flow

The historical backfill uses the full-load Glue jobs:

```text
bronze_ingestion
        ↓
silver_cleaned
        ↓
data_quality_check
        ↓
gold_layer
```

The complete flow is:

```text
Backblaze Historical Release
        ↓
      S3 RAW
        ↓
bronze_ingestion
        ↓
      Bronze
        ↓
  silver_cleaned
        ↓
      Silver
        ↓
data_quality_check
        ↓
  Data Quality
        ↓
     gold_layer
        ↓
      Gold
```

Each stage has a different responsibility.

```text
bronze_ingestion
→ Loads the selected historical source data

silver_cleaned
→ Applies the canonical cleaning and transformation logic

data_quality_check
→ Checks whether the processed data meets the required quality rules

gold_layer
→ Produces the analytical output
```

---

## 3.5 🔐 Why I Process the History in Release-Level Units

Processing the whole historical dataset as one giant uncontrolled job would make failures and reruns harder to manage.

Instead, the project uses release-level units.

```text
2013
   ↓
Process
   ↓
Complete

2014
   ↓
Process
   ↓
Complete

2015
   ↓
Process
   ↓
Complete

2016_Q1
   ↓
Process
   ↓
Complete
```

This gives each part of the history its own processing boundary.

If a release fails, I can identify exactly which release needs attention instead of treating the entire historical dataset as one failed workload.

---

## 3.6 📊 What This Backfill Achieves

The historical backfill converts the existing Backblaze history into the lakehouse layers used by the project.

Before the backfill:

```text
Backblaze Historical Data
        ↓
Mostly just source files
```

After the backfill:

```text
Backblaze Historical Data
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

The result is a historical baseline covering the project's:

```text
2013 → 2026_Q1
```

scope.

Once that baseline exists, the project no longer needs to repeatedly rebuild the entire historical dataset for every new file.

---

## 3.7 🔄 What Happens After the Backfill

After the historical baseline is complete, the operating model changes.

The project moves from:

```text
Historical Backfill
```

to:

```text
Event-Driven Incremental Processing
```

The difference is:

```text
Historical
→ Process an explicitly selected release

Incremental
→ Process a newly arrived file
```

So the historical backfill is not a separate project from the event-driven pipeline.

It is the first stage of the same lakehouse.

```text
Historical Backfill
        ↓
Build Baseline
        ↓
Event-Driven Incremental Processing
        ↓
Keep Baseline Updated
```

That is how the historical load solves the initial 229 GB data problem while still giving the project a clean path into ongoing daily ingestion.
---

# 4. ⚡ Incremental Event-Driven Processing

After the historical baseline is established, the project moves to incremental processing.

At this point, the problem is different from the historical backfill.

The historical pipeline processes data that already exists.

The incremental pipeline waits for new Backblaze CSV files and starts processing when a new file arrives.

The flow is:

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
AWS Glue
```

The S3 event is configured for the RAW Backblaze path:

```text
Prefix:
raw/drivestats/

Suffix:
.csv
```

This means a matching CSV object created in the RAW location becomes an input to the event-driven pipeline.

---

## 4.1 📦 Incremental Processing Unit

The incremental pipeline is **file-scoped**.

The new source file is the processing unit.

For example:

```text
2026-03-31.csv
```

becomes:

```text
1 Source File
      ↓
1 S3 Event
      ↓
1 SQS Message
      ↓
1 Processing Unit
```

The pipeline does not treat the entire RAW dataset as one incremental workload.

It processes the specific file that triggered the event.

---

## 4.2 🔧 Incremental Glue Jobs

The event-driven workflow uses four incremental Glue jobs:

```text
bronze_layer
silver_layer
data_quality_layer
gold_analytics_layer
```

The processing sequence is:

```text
New Source File
        ↓
bronze_layer
        ↓
Bronze
        ↓
silver_layer
        ↓
Silver
        ↓
data_quality_layer
        ↓
Data Quality
        ↓
gold_analytics_layer
        ↓
Gold
```

Each job has a specific role in the incremental path.

```text
bronze_layer
→ Processes the newly arrived source file

silver_layer
→ Applies the canonical transformation logic

data_quality_layer
→ Validates the processed data

gold_analytics_layer
→ Builds the analytical output
```

---

## 4.3 🎯 Explicit Input Scope

The incremental jobs are also designed with explicit processing scope.

Bronze, Silver, and Data Quality receive:

```text
--input_path
--release_id
```

For example:

```text
--input_path s3://.../raw/drivestats/.../2026-03-31.csv
--release_id <release_id>
```

This tells the jobs exactly which source file they are expected to process.

Gold receives:

```text
--release_id
```

because Gold works from the processed release data rather than directly from the raw source file.

The important design decision is:

```text
Incremental Processing
→ Process the required file

Not:

→ Scan the entire RAW dataset
```

---

## 4.4 🔄 Historical vs Incremental

The two operating modes now have clear responsibilities.

```text
Historical
→ Release-oriented backfill
→ Builds the initial lakehouse baseline

Incremental
→ File-oriented processing
→ Keeps the lakehouse updated
```

The complete project flow is therefore:

```text
Historical Backfill
        ↓
Historical Baseline
        ↓
New Backblaze File Arrives
        ↓
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
Incremental Glue
        ↓
Bronze
        ↓
Silver
        ↓
Data Quality
        ↓
Gold
```

This is the event-driven part of the project that takes over after the historical backfill.

# 5. 🗂️ Data Plane

The data plane is the part of the project that actually processes the Backblaze data.

The control plane decides **what file should be processed and when it should run**.

The data plane is responsible for **reading that file, transforming it, validating it, and producing the final analytical data**.

The main data flow is:

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

I use the following technologies in the data plane:

```text
Amazon S3
AWS Glue
Apache Spark
Apache Iceberg
AWS Glue Data Catalog
Amazon Athena
Amazon QuickSight
```

Each one solves a different problem.

| Technology | How I use it | What problem it solves |
|---|---|---|
| Amazon S3 | Stores the raw Backblaze files and lakehouse data | Durable and scalable storage |
| AWS Glue | Runs the Spark jobs | Managed execution without maintaining Spark servers |
| Apache Spark | Reads and transforms the data | Distributed processing for large datasets |
| Apache Iceberg | Stores Bronze, Silver and Gold tables | Gives the S3 data a proper table structure |
| Glue Data Catalog | Keeps track of Iceberg tables | Gives Spark and Athena a common table catalog |
| Athena | Queries and validates the lakehouse | Lets me inspect and test the data with SQL |
| QuickSight | Uses the analytical output | Provides the reporting and dashboard layer |

The important point is that these services are not being used independently.

They work together as one data-processing path.

---

# 6. ☁️ Amazon S3

I use Amazon S3 as the main storage layer.

In this project, S3 is basically the data lake.

The first important responsibility of S3 is storing the original Backblaze files.

The RAW path is:

```text
raw/drivestats/
```

This is where the source files land before the processing layers use them.

The project also uses S3 for other project objects such as:

```text
glue_scripts/
iceberg/
schemas/
reports/
architecture/
dashboard/
```

The most important part of the RAW layer is that the original source file remains available.

For example:

```text
Backblaze CSV
      ↓
S3 RAW
      ↓
Bronze
      ↓
Silver
```

If something fails later:

```text
Silver ❌
```

or:

```text
Gold ❌
```

the original source file is still in S3.

That means I can investigate the failure or process the source again without needing to download the source data again.

S3 also plays another role in the project.

It is the starting point of the incremental event-driven pipeline.

When a new CSV is created under the configured RAW path, S3 generates the event that starts the control-plane workflow.

So S3 has two important responsibilities here:

```text
1. Store the source data
2. Generate the event when new data arrives
```

---

# 7. 🧱 Bronze Layer

Bronze is the first processing layer after RAW.

The purpose of Bronze is to bring the source data into the lakehouse without immediately mixing in all of the business logic.

The idea is:

```text
RAW
 ↓
Bronze
```

Bronze stays close to the source representation.

It also carries the processing metadata needed to understand where the data came from and which processing run created it.

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

The `--input_path` is important because the incremental pipeline is file-scoped.

It tells Bronze:

```text
"Process this source file."
```

instead of:

```text
"Read everything under RAW."
```

Bronze is stored using Apache Iceberg.

The main problem Bronze solves is creating a durable lakehouse representation of the source before applying the more controlled transformations in Silver.

The basic responsibility is:

```text
S3 RAW
   ↓
Read Source
   ↓
Add Processing Metadata
   ↓
Bronze Iceberg Table
```

---

# 8. 🧹 Silver Layer

Silver is where the source-oriented Bronze data is turned into a controlled structure for downstream use.

The flow is:

```text
Bronze
   ↓
Silver
```

This is where the project applies the canonical transformation logic.

The historical Silver job is:

```text
silver_cleaned
```

The incremental Silver job is:

```text
silver_layer
```

For incremental processing it receives:

```text
--input_path
--release_id
```

Silver handles the things that should not be pushed into the source-oriented Bronze layer.

This includes areas such as:

```text
Column mapping
Type casting
Required-field handling
Deduplication
Canonical schema handling
Quarentine bad records instead of deleting
Schema differences between releases
```

The important reason for doing this in Silver is that the Backblaze source structure is not guaranteed to remain exactly the same across all releases.

I therefore use:

```text
Source Structure
      ↓
Bronze
      ↓
Canonical Transformation
      ↓
Silver
```

That gives downstream layers a more consistent structure to work with.

The main problem Silver solves is:

```text
How do I take changing source data
and turn it into a controlled structure
that the rest of the pipeline can use?
```

---

# 9. ✅ Data Quality Layer

The project has a separate Data Quality stage.

I kept Data Quality separate from the main transformation logic because:

```text
A job completing successfully
does not automatically mean
the data is correct.
```

The historical DQ job is:

```text
data_quality_check
```

The incremental DQ job is:

```text
data_quality_layer
```

The stage runs after Silver:

```text
Silver
   ↓
Data Quality
   ↓
Gold
```

The DQ stage checks the processed dataset against the required quality rules.

The project also includes quarantine handling.

This means invalid records do not have to simply disappear.

They can be isolated with the information needed to understand why they were rejected.

So the DQ stage solves a different problem from Silver.

```text
Silver
→ "Can I transform and standardize this data?"

Data Quality
→ "Is the resulting data acceptable for downstream use?"
```

This gives the pipeline an explicit quality checkpoint before Gold.

---

# 10. 🏆 Gold Layer

Gold is the final analytical layer of the lakehouse.

The flow is:

```text
Bronze
   ↓
Silver
   ↓
Data Quality
   ↓
Gold
```

The historical Gold job is:

```text
gold_layer
```

The incremental Gold job is:

```text
gold_analytics_layer
```

By the time the data reaches Gold, the major source-handling work has already happened.

The Gold layer can therefore focus on producing data that is easier to use for analytics.

The purpose is no longer:

```text
"Keep the source data."
```

The purpose is:

```text
"Produce useful analytical data."
```

The Gold layer is then consumed by the analytical side of the project.

---

# 11. 🧊 Apache Iceberg

I use Apache Iceberg as the table format for the lakehouse.

The important difference is that I am not treating the lakehouse as just a collection of Parquet files sitting in S3.

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

S3 stores the underlying data.

Iceberg provides the table abstraction over that storage.

That allows the project to work with proper lakehouse tables instead of manually managing files as if they were database tables.

The main processing layers are:

```text
Bronze
Silver
Gold
```

All three are represented as lakehouse tables using Iceberg.

So the roles are separated:

```text
S3
→ Stores the physical data

Iceberg
→ Defines the table structure and table state

Glue Data Catalog
→ Makes those tables discoverable

Spark / Athena
→ Read and work with the tables
```

This solves one of the main problems of building a lakehouse on object storage:

```text
How do I turn files in S3
into tables that data tools can reliably work with?
```

---

# 12. ⚙️ Apache Spark and AWS Glue

I use Apache Spark for the actual distributed data processing.

I run Spark through AWS Glue.

That means I do not have to manage my own Spark cluster just to run these ETL jobs.

The relationship is:

```text
AWS Glue
   ↓
Apache Spark
   ↓
Process Data
   ↓
Apache Iceberg
```

Glue provides the managed execution environment.

Spark performs the actual distributed processing.

The project separates the processing into different jobs:

```text
Bronze
Silver
Data Quality
Gold
```

That separation is important because each stage has a different responsibility.

It also allows Step Functions to control the stages independently.

For example:

```text
Bronze succeeds
      ↓
Start Silver
```

If Silver fails:

```text
Silver ❌
      ↓
Do not continue blindly to Gold
```

So Glue is the compute layer.

Spark is the processing engine.

Iceberg is the table layer.

---

# 13. 🔗 How the Data Plane Connects Together

The data plane is not a collection of unrelated jobs.

The output of one stage becomes the input to the next stage.

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

Each stage has a reason.

```text
RAW
→ Keep the original source

Bronze
→ Ingest the source into the lakehouse

Silver
→ Standardize and transform the data

Data Quality
→ Check whether the processed data is acceptable

Gold
→ Produce analytical data
```

The result is a controlled progression from source data to analytical data.

---

# 14. 🔍 Amazon Athena

I use Amazon Athena to query the lakehouse tables.

Athena is especially useful during validation because I can inspect the actual data after a Glue job finishes.

For example, I use Athena for:

```text
Row-count checks
Schema checks
Release validation
Data Quality inspection
Bronze validation
Silver validation
Gold analysis
```

This gives me a way to answer questions such as:

```text
Did the expected data arrive?

Did the transformation produce the expected rows?

Does the table have the expected schema?

Did the release finish correctly?

Does Gold contain the expected analytical output?
```

Athena therefore acts as both a query layer and a validation tool in the project.

---

# 15. 📊 Amazon QuickSight

I use Amazon QuickSight as the reporting and dashboard layer.

The dashboard is built from the processed analytical data rather than directly from the RAW files.

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

This keeps the dashboard separate from ingestion and transformation.

QuickSight is therefore not part of the ingestion pipeline itself.

It is the consumption layer that sits after the Gold data has been prepared.

---

# 16. 🎯 What the Data Plane Solves

The data plane solves the actual data problem.

It takes:

```text
Raw Backblaze Files
```

and turns them into:

```text
Validated Analytical Data
```

The full path is:

```text
RAW
 ↓
Bronze
 ↓
Silver
 ↓
Data Quality
 ↓
Gold
```

Each layer removes a different problem.

```text
RAW
→ Source storage

Bronze
→ Durable ingestion

Silver
→ Standardization and transformation

Data Quality
→ Validation

Gold
→ Analytical output
```

This is the main purpose of the data plane.

The control plane will handle the question of **when and how this processing should run**.
