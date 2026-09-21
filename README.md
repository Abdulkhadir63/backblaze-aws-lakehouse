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

# 1. 📌 About This Project

I built this project around the **Backblaze Drive Stats** dataset.

Backblaze publishes hard-drive statistics as CSV files. The data is released over time, so there is a large amount of historical data and new files can continue to arrive.

The main goal of this project is not just to read those CSV files with Spark.

I wanted to build the complete data pipeline around them.

That means the project has to handle both sides of the problem:

```text
HISTORICAL DATA
      ↓
Build the lakehouse
```

and later:

```text
NEW DATA ARRIVES
      ↓
Detect it
      ↓
Process it
      ↓
Update the lakehouse
```

The project currently uses a historical backfill of about:

```text
228 files
~229 GB
```

After the historical data is loaded, the pipeline is designed to handle new Backblaze CSV files as they arrive.

So the project is built around this simple idea:

```text
First build the history.

Then keep the history updated.
```

---

# 2. 🎯 What I Wanted to Build

I did not want this project to stop at:

```text
CSV
 ↓
Spark
 ↓
Table
```

That would only solve the data transformation part.

I also wanted to solve the problems that appear when the pipeline is actually running.

For example:

```text
What happens when a new file arrives?

How does the pipeline know that the file arrived?

What happens if the same event is received twice?

How do I know which file is being processed?

How do I stop two processing runs from fighting over the same pipeline?

What happens when Silver fails?

How do I know which stage failed?

How do I process the file again?

Where do I keep the original data?

How do I handle schema changes between Backblaze releases?

How do I validate the data before sending it to Gold?

How do I deploy the whole system without manually creating everything again?
```

Those questions are what drove the architecture.

---

# 3. 🏗️ The Project in Simple Terms

At the highest level, the project has two sides.

The first side is the **data processing**:

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

The second side is the **control and orchestration**:

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

I use the first side to process the data.

I use the second side to control when and how that processing happens.

---

# 4. 🔄 The Main Idea Behind the Whole Project

The project starts with historical data.

```text
Historical Backblaze Data
        ↓
Historical Backfill
        ↓
Lakehouse Baseline
```

After that, new data becomes the problem.

```text
New Backblaze CSV
        ↓
File Arrives
        ↓
Event Is Created
        ↓
Pipeline Reacts
        ↓
File Is Processed
        ↓
Lakehouse Is Updated
```

So the full lifecycle is:

```text
HISTORICAL BACKFILL
        ↓
HISTORICAL BASELINE
        ↓
NEW DATA ARRIVES
        ↓
EVENT-DRIVEN PROCESSING
        ↓
UPDATED LAKEHOUSE
```

That is the basic idea of the project.

The rest of this README explains how I built each part and why I made those design decisions.
