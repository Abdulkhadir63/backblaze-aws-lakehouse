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
  Historical Backfill • Incremental Processing • Event-Driven Pipeline • Apache Iceberg • AWS Glue • Spark • Lambda • SQS • DynamoDB • Step Functions • Terraform • GitHub Actions
</p>

---

# **BACKBLAZE AWS MODERN DATA LAKEHOUSE PROJECT**

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

I built this project around the Backblaze Drive Stats dataset.

The dataset contains a large amount of historical hard-drive statistics collected over time. The data is useful for analytics, but the real engineering problem is building a pipeline that can process this data reliably and continue processing new files without manual intervention.

This project is built to solve that problem.

The system has to deal with two different situations:

```text
1. Historical data already exists
2. New data arrives over time
```

These two situations are not the same.

Historical data needs a controlled backfill so I can establish the initial state of the lakehouse.

New data needs an incremental, event-driven pipeline so the system can react when a new source file arrives.

The pipeline therefore needs to answer practical engineering questions:

```text
How do I load a large historical dataset without treating every run
as a completely new pipeline?

How do I process a new CSV when it arrives without manually
starting a Glue job?

What happens when the same S3 event is delivered more than once?

How do I know which file is currently being processed?

How do I prevent two files from being processed at the same time
when the pipeline is designed around one active processing unit?

Where do I preserve the original source data?

Where do I handle changing source schemas?

What happens when records fail validation?

How do I coordinate Bronze → Silver → Data Quality → Gold?

What happens when one processing stage fails?

How do I resume processing after a failure?

How do I deploy infrastructure and application code
without manually rebuilding everything?
```

I built the project to handle these problems as one system.

The result is not simply:

```text
CSV
  ↓
Spark
  ↓
Table
```

Instead, the project is split into two connected parts:

```text
DATA PLANE
→ Processes the actual data

CONTROL PLANE
→ Controls what should be processed,
  when it should be processed,
  which file should be processed,
  and what happened to that file
```

The data plane handles the actual lakehouse processing:

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

The control plane handles the workflow around that processing:

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

The two planes have different responsibilities.

The data plane answers:

```text
"How should the data be processed?"
```

The control plane answers:

```text
"What work should happen,
for which file,
when,
and what is the current state of that work?"
```

This separation is important because the project is not only a Spark transformation pipeline.

It also needs to manage:

```text
Source file registration
       ↓
Processing state
       ↓
File ownership
       ↓
Orchestration
       ↓
Failure handling
       ↓
Resume handling
       ↓
Successful completion
```

The project therefore treats data processing and pipeline control as separate concerns that work together.

The final goal is simple:

```text
Build the historical lakehouse
            ↓
Establish a known baseline
            ↓
Detect new files automatically
            ↓
Process each new file
            ↓
Track its processing state
            ↓
Handle failures and resume when possible
            ↓
Keep the lakehouse updated
```

This is the core problem the project is designed to solve.
