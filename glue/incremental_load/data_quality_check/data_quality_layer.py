import sys
import uuid
import boto3
import yaml

from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.transforms import SelectFromCollection
from awsglue import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F


# =========================================================
# 1. INITIALIZATION
# =========================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "release_id"
    ]
)


def get_optional_arg(argument_name):

    flag = f"--{argument_name}"

    for index, value in enumerate(sys.argv):

        if value == flag:

            if index + 1 >= len(sys.argv):

                raise ValueError(
                    f"Missing value for {flag}"
                )

            return sys.argv[index + 1]

    return None


input_path = get_optional_arg(
    "input_path"
)


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# =========================================================
# 2. PROJECT CONFIGURATION
# =========================================================

PROJECT_BUCKET = "backblaze-de-lakehouse-project"

release_id = args["release_id"]
run_id = str(uuid.uuid4())

silver_table = (
    "glue_catalog.backblaze_db.silver_drivestats"
)

bronze_table = (
    "glue_catalog.backblaze_db.bronze_drivestats"
)

dq_results_table = (
    "glue_catalog.backblaze_db.dq_results"
)

schema_s3_uri = (
    f"s3://{PROJECT_BUCKET}/"
    "schemas/drivestats_v1.yaml"
)


# =========================================================
# 2A. VALIDATE INCREMENTAL INPUT PATH
# =========================================================

if input_path is not None:

    expected_prefix = (
        f"s3://{PROJECT_BUCKET}/"
        "raw/drivestats/"
    )

    if not input_path.startswith(
        expected_prefix
    ):

        raise ValueError(
            "Invalid --input_path. "
            "Input must be under "
            f"{expected_prefix}. "
            f"Received: {input_path}"
        )

    if not input_path.lower().endswith(
        ".csv"
    ):

        raise ValueError(
            "Invalid --input_path. "
            "Incremental input must be a CSV file."
        )

    if "*" in input_path:

        raise ValueError(
            "Wildcard input paths are not allowed."
        )


# =========================================================
# 3. CANONICAL SCHEMA HELPERS
# =========================================================

def parse_s3_uri(s3_uri):

    if not s3_uri.startswith("s3://"):

        raise ValueError(
            f"Invalid S3 URI: {s3_uri}"
        )

    path = s3_uri[5:]

    if "/" not in path:

        raise ValueError(
            f"Invalid S3 URI: {s3_uri}"
        )

    bucket, key = path.split(
        "/",
        1
    )

    return bucket, key


def load_s3_text(s3_uri):

    bucket, key = parse_s3_uri(
        s3_uri
    )

    s3 = boto3.client("s3")

    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    return (
        response["Body"]
        .read()
        .decode("utf-8")
    )


def load_yaml_from_s3(s3_uri):

    content = load_s3_text(
        s3_uri
    )

    return yaml.safe_load(
        content
    )


def normalize_datatype(datatype):

    if datatype is None:

        return None

    value = (
        str(datatype)
        .strip()
        .lower()
    )

    aliases = {

        "integer": "int",
        "int": "int",

        "long": "bigint",
        "bigint": "bigint",

        "short": "smallint",
        "smallint": "smallint",

        "byte": "tinyint",
        "tinyint": "tinyint",

        "double": "double",
        "float": "float",

        "string": "string",

        "boolean": "boolean",
        "bool": "boolean",

        "date": "date",

        "timestamp": "timestamp",

        "binary": "binary"
    }

    return aliases.get(
        value,
        value
    )


# =========================================================
# 4. LOAD CANONICAL SCHEMA
# =========================================================

schema_config = load_yaml_from_s3(
    schema_s3_uri
)

canonical_columns = (
    schema_config.get(
        "columns",
        []
    )
)


if not canonical_columns:

    raise ValueError(
        "Canonical schema contains no columns."
    )


expected_schema = {

    field["name"]:
        normalize_datatype(
            field["datatype"]
        )

    for field in canonical_columns

}


# =========================================================
# 5. OPERATIONAL METADATA COLUMNS
# =========================================================

operational_columns = {

    "release_id",
    "source_file",
    "ingested_at",
    "bronze_ingested_at",
    "processing_run_id",
    "silver_processed_at"

}


# =========================================================
# 6. READ SILVER WITH EXPLICIT SCOPE
# =========================================================
#
# BACKFILL:
#     release_id scope
#
# INCREMENTAL:
#     exact source_file scope
# =========================================================

if input_path is not None:

    df_silver = (

        spark.table(
            silver_table
        )

        .filter(
            F.col("source_file")
            ==
            F.lit(input_path)
        )

    )

else:

    df_silver = (

        spark.table(
            silver_table
        )

        .filter(
            F.col("release_id")
            ==
            F.lit(release_id)
        )

    )


# =========================================================
# 7. CUSTOM DATASET AGGREGATION
# =========================================================

business_key_columns = [

    "release_id",
    "snapshot_date",
    "drive_serial_number"

]


aggregate_row = (

    df_silver

    .agg(

        F.count("*").alias(
            "total_rows"
        ),

        F.countDistinct(

            *[
                F.col(column)
                for column
                in business_key_columns
            ]

        ).alias(
            "distinct_business_keys"
        ),

        F.sum(

            F.when(

                F.col(
                    "drive_serial_number"
                ).isNull(),

                1

            ).otherwise(0)

        ).alias(
            "null_drive_serial_number"
        ),

        F.sum(

            F.when(

                F.col(
                    "drive_model"
                ).isNull(),

                1

            ).otherwise(0)

        ).alias(
            "null_drive_model"
        ),

        F.sum(

            F.when(

                F.col(
                    "snapshot_date"
                ).isNull(),

                1

            ).otherwise(0)

        ).alias(
            "null_snapshot_date"
        )

    )

    .collect()[0]

)


total_rows = (
    aggregate_row["total_rows"]
)

distinct_business_keys = (
    aggregate_row[
        "distinct_business_keys"
    ]
)

null_drive_serial_number = (
    aggregate_row[
        "null_drive_serial_number"
    ]
    or 0
)

null_drive_model = (
    aggregate_row[
        "null_drive_model"
    ]
    or 0
)

null_snapshot_date = (
    aggregate_row[
        "null_snapshot_date"
    ]
    or 0
)


# =========================================================
# 8. DQ RESULTS COLLECTION
# =========================================================

dq_results = []


def add_result(
    check_name,
    result,
    details
):

    dq_results.append(

        (
            check_name,
            result,
            details,
            run_id,
            datetime.now(
                timezone.utc
            ),
            release_id
        )

    )


# =========================================================
# 9. EMPTY DATASET CHECK
# =========================================================

if total_rows == 0:

    if input_path is not None:

        scope_message = (
            "No Silver rows found for "
            f"input_path={input_path}."
        )

    else:

        scope_message = (
            "No Silver rows found for "
            f"release_id={release_id}."
        )


    add_result(

        check_name="row_count",

        result="FAIL",

        details=scope_message

    )

else:

    add_result(

        check_name="row_count",

        result="PASS",

        details=(
            f"silver_rows={total_rows}"
        )

    )


# =========================================================
# 10. AWS GLUE DATA QUALITY
# =========================================================

if total_rows > 0:

    dq_ruleset = """
    Rules = [

        ColumnExists "drive_serial_number",
        ColumnExists "drive_model",
        ColumnExists "snapshot_date",

        Completeness "drive_serial_number" = 1,
        Completeness "drive_model" = 1,
        Completeness "snapshot_date" = 1,

        RowCount > 0
    ]
    """


    silver_dynamic_frame = (
        DynamicFrame.fromDF(
            df_silver,
            glueContext,
            "silver_dq_input"
        )
    )


    dq_evaluation = (

        EvaluateDataQuality()

        .process_rows(

            frame=
                silver_dynamic_frame,

            ruleset=
                dq_ruleset,

            publishing_options={

                "dataQualityEvaluationContext":
                    f"backblaze_silver_{release_id}",

                "enableDataQualityCloudWatchMetrics":
                    True,

                "enableDataQualityResultsPublishing":
                    True

            },

            additional_options={

                "performanceTuning.caching":
                    "CACHE_NOTHING"

            }

        )

    )


    rule_outcomes = (

        SelectFromCollection.apply(

            dfc=
                dq_evaluation,

            key=
                "ruleOutcomes",

            transformation_ctx=
                "rule_outcomes"

        )

    )


    df_rule_outcomes = (
        rule_outcomes.toDF()
    )


    glue_rule_rows = (

        df_rule_outcomes

        .select(

            "Rule",
            "Outcome",
            "FailureReason",
            "EvaluatedMetrics"

        )

        .collect()

    )


    for row in glue_rule_rows:

        outcome = str(
            row["Outcome"]
        ).upper()


        if outcome == "PASSED":

            result = "PASS"

        elif outcome in {
            "FAILED",
            "ERROR"
        }:

            result = "FAIL"

        else:

            result = "WARNING"


        details = (

            f"rule={row['Rule']}; "

            f"failure_reason="
            f"{row['FailureReason']}; "

            f"metrics="
            f"{row['EvaluatedMetrics']}"

        )


        add_result(

            check_name=(
                f"GLUE_DQ::{row['Rule']}"
            ),

            result=result,

            details=details

        )


# =========================================================
# 11. BUSINESS KEY UNIQUENESS
# =========================================================

if total_rows > 0:

    if (
        total_rows
        ==
        distinct_business_keys
    ):

        add_result(

            check_name=
                "business_key_uniqueness",

            result="PASS",

            details=(

                f"rows={total_rows}; "
                f"distinct_keys="
                f"{distinct_business_keys}"

            )

        )

    else:

        duplicate_rows = (

            total_rows
            -
            distinct_business_keys

        )


        add_result(

            check_name=
                "business_key_uniqueness",

            result="FAIL",

            details=(

                f"rows={total_rows}; "

                f"distinct_keys="
                f"{distinct_business_keys}; "

                f"duplicate_rows="
                f"{duplicate_rows}"

            )

        )


# =========================================================
# 12. CRITICAL NULL RATE
# =========================================================

critical_null_metrics = {

    "drive_serial_number":
        null_drive_serial_number,

    "drive_model":
        null_drive_model,

    "snapshot_date":
        null_snapshot_date

}


for (
    column_name,
    null_count
) in critical_null_metrics.items():

    if total_rows > 0:

        null_rate = (

            float(null_count)
            /
            float(total_rows)

        )

    else:

        null_rate = 1.0


    if null_rate == 0.0:

        result = "PASS"

    else:

        result = "FAIL"


    add_result(

        check_name=(
            f"null_rate::{column_name}"
        ),

        result=result,

        details=(

            f"null_count={null_count}; "

            f"total_rows={total_rows}; "

            f"null_rate="
            f"{null_rate:.6f}"

        )

    )


# =========================================================
# 13. CANONICAL SCHEMA COMPATIBILITY
# =========================================================

actual_schema = {

    field.name:

        normalize_datatype(

            field.dataType.simpleString()

        )

    for field
    in df_silver.schema.fields

}


missing_canonical_columns = [

    column_name

    for column_name
    in expected_schema

    if column_name
    not in actual_schema

]


unexpected_columns = [

    column_name

    for column_name
    in actual_schema

    if (

        column_name
        not in expected_schema

        and

        column_name
        not in operational_columns

    )

]


datatype_mismatches = []


for (
    column_name,
    expected_type
) in expected_schema.items():

    if (
        column_name
        not in actual_schema
    ):

        continue


    actual_type = (
        actual_schema[
            column_name
        ]
    )


    if actual_type != expected_type:

        datatype_mismatches.append(

            (
                f"{column_name}: "
                f"expected={expected_type}, "
                f"actual={actual_type}"
            )

        )


if (

    not missing_canonical_columns

    and

    not unexpected_columns

    and

    not datatype_mismatches

):

    add_result(

        check_name=
            "schema_compatibility",

        result="PASS",

        details=(

            "All canonical columns exist "
            "with compatible datatypes. "
            "Known operational metadata "
            "columns are permitted."

        )

    )

else:

    add_result(

        check_name=
            "schema_compatibility",

        result="FAIL",

        details=(

            f"missing_canonical="
            f"{missing_canonical_columns}; "

            f"unexpected_columns="
            f"{unexpected_columns}; "

            f"type_mismatches="
            f"{datatype_mismatches}"

        )

    )


# =========================================================
# 14. BRONZE → SILVER RECONCILIATION
# =========================================================
#
# IMPORTANT:
# Reconciliation MUST use the same processing scope
# as the Silver input.
#
# Otherwise incremental DQ would compare:
#
#     one day's Silver
#     against
#     the entire release's Bronze
#
# which would be meaningless.
# =========================================================

if input_path is not None:

    bronze_count = (

        spark.table(
            bronze_table
        )

        .filter(

            F.col(
                "source_file"
            )
            ==
            F.lit(input_path)

        )

        .count()

    )

else:

    bronze_count = (

        spark.table(
            bronze_table
        )

        .filter(

            F.col("release_id")
            ==
            F.lit(release_id)

        )

        .count()

    )


count_difference = (
    bronze_count
    -
    total_rows
)


if count_difference >= 0:

    reconciliation_result = (
        "PASS"
    )

else:

    reconciliation_result = (
        "FAIL"
    )


add_result(

    check_name=
        "bronze_silver_row_reconciliation",

    result=
        reconciliation_result,

    details=(

        f"bronze_rows={bronze_count}; "

        f"silver_rows={total_rows}; "

        f"difference={count_difference}; "

        "Silver may be lower because of "
        "quarantine and deduplication."

    )

)


# =========================================================
# 15. OVERALL DQ STATUS
# =========================================================

result_values = [

    result[1]

    for result
    in dq_results

]


if "FAIL" in result_values:

    overall_status = "FAIL"

elif "WARNING" in result_values:

    overall_status = "WARNING"

else:

    overall_status = "PASS"


# =========================================================
# 16. CREATE RESULT DATAFRAME
# =========================================================

dq_schema = [

    "check_name",
    "result",
    "details",
    "run_id",
    "timestamp",
    "release_id"

]


df_dq_results = spark.createDataFrame(

    dq_results,

    dq_schema

)


# =========================================================
# 17. CREATE OR APPEND DQ RESULT TABLE
# =========================================================

if not spark.catalog.tableExists(

    dq_results_table

):

    (

        df_dq_results

        .writeTo(
            dq_results_table
        )

        .tableProperty(
            "format-version",
            "2"
        )

        .tableProperty(
            "write.format.default",
            "parquet"
        )

        .tableProperty(
            "write.parquet.compression-codec",
            "snappy"
        )

        .partitionedBy(
            "release_id"
        )

        .create()

    )

    print(
        "DQ results table created."
    )

else:

    (

        df_dq_results

        .writeTo(
            dq_results_table
        )

        .append()

    )

    print(
        "DQ results appended."
    )


# =========================================================
# 18. PRINT FINAL DQ REPORT
# =========================================================

print(
    "============================================"
)

print(
    "BACKBLAZE DATA QUALITY RESULTS"
)

print(
    "============================================"
)

print(
    f"Release ID : {release_id}"
)

if input_path is not None:

    print(
        f"Input Path : {input_path}"
    )

print(
    f"Run ID     : {run_id}"
)

print(
    f"Overall    : {overall_status}"
)

print(
    f"Silver Rows: {total_rows}"
)

print(
    "--------------------------------------------"
)


for result in dq_results:

    print(

        f"{result[0]} | "
        f"{result[1]} | "
        f"{result[2]}"

    )


print(
    "--------------------------------------------"
)

print(
    f"DQ Results Table: "
    f"{dq_results_table}"
)

print(
    "============================================"
)


# =========================================================
# 19. FAIL THE GLUE JOB AFTER RESULTS ARE PERSISTED
# =========================================================

if overall_status == "FAIL":

    job.commit()

    raise RuntimeError(

        "Data quality FAILED for "

        f"release_id={release_id}; "

        f"run_id={run_id}"

    )


# =========================================================
# 20. SUCCESSFUL JOB COMMIT
# =========================================================

job.commit()

print(
    "============================================"
)

print(
    "DQ JOB COMPLETED"
)

print(
    "============================================"
)