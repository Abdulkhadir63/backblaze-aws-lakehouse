import sys
import uuid
import boto3
import yaml

from pyspark.sql.functions import months
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------
# 1. INITIALIZATION & PARAMETERS
# ---------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "release_id"]
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


release_id = args["release_id"]

input_path = get_optional_arg(
    "input_path"
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# 2. PROJECT METADATA
# ---------------------------------------------------------
run_id = str(uuid.uuid4())

bronze_table = (
    "glue_catalog.backblaze_db.bronze_drivestats"
)

silver_table = (
    "glue_catalog.backblaze_db.silver_drivestats"
)

quarantine_table = (
    "glue_catalog.backblaze_db.quarantine_drivestats"
)

schema_s3_uri = (
    "s3://backblaze-de-lakehouse-project/"
    "schemas/drivestats_v1.yaml"
)


# ---------------------------------------------------------
# 2A. VALIDATE INCREMENTAL INPUT PATH
# ---------------------------------------------------------
if input_path is not None:

    expected_prefix = (
        "s3://backblaze-de-lakehouse-project/"
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

    if not input_path.lower().endswith(".csv"):

        raise ValueError(
            "Invalid --input_path. "
            "Incremental input must be a CSV file."
        )

    if "*" in input_path:

        raise ValueError(
            "Wildcard input paths are not allowed."
        )


# ---------------------------------------------------------
# 3. LOAD CANONICAL SCHEMA FROM S3
# ---------------------------------------------------------
def parse_s3_uri(s3_uri):

    if not s3_uri.startswith("s3://"):

        raise ValueError(
            f"Invalid S3 URI: {s3_uri}"
        )

    path = s3_uri[5:]

    bucket, key = path.split(
        "/",
        1
    )

    return bucket, key


def load_yaml_from_s3(s3_uri):

    bucket, key = parse_s3_uri(
        s3_uri
    )

    s3 = boto3.client("s3")

    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    content = (
        response["Body"]
        .read()
        .decode("utf-8")
    )

    return yaml.safe_load(
        content
    )


schema_config = load_yaml_from_s3(
    schema_s3_uri
)

canonical_columns = (
    schema_config["columns"]
)


if not canonical_columns:

    raise ValueError(
        "Canonical schema contains no columns."
    )


# ---------------------------------------------------------
# 4. READ BRONZE
# ---------------------------------------------------------
#
# BACKFILL:
#     process the requested release.
#
# INCREMENTAL:
#     process only Bronze rows produced from
#     the exact source file.
# ---------------------------------------------------------

df_bronze_base = (
    spark.table(
        bronze_table
    )
)


if input_path is not None:

    df_bronze = (
        df_bronze_base
        .filter(
            F.col("source_file")
            ==
            F.lit(input_path)
        )
    )

else:

    df_bronze = (
        df_bronze_base
        .filter(
            F.col("release_id")
            ==
            F.lit(release_id)
        )
    )


# ---------------------------------------------------------
# 4A. FAIL EARLY IF REQUESTED SCOPE IS EMPTY
# ---------------------------------------------------------
if (
    df_bronze
    .limit(1)
    .count()
    == 0
):

    if input_path is not None:

        raise ValueError(
            "No Bronze data found for "
            f"input_path={input_path}"
        )

    raise ValueError(
        "No Bronze data found for "
        f"release_id={release_id}"
    )


# ---------------------------------------------------------
# 5. BUILD CANONICAL SILVER EXPRESSIONS
# ---------------------------------------------------------
from pyspark.storagelevel import StorageLevel


def build_source_expression(
    field,
    source_columns
):

    target = field["name"]

    source = field.get(
        "source_column"
    )

    datatype = field[
        "datatype"
    ]

    transformation = field.get(
        "transformation"
    )

    if source not in source_columns:

        if field["nullable"]:

            return (
                F.lit(None)
                .cast(datatype)
                .alias(target)
            )

        raise ValueError(
            f"Required source column "
            f"'{source}' is missing "
            f"for canonical field "
            f"'{target}'"
        )

    source_col = F.col(
        source
    )

    if target in {
        "drive_serial_number",
        "drive_model"
    }:

        source_col = F.trim(
            source_col
        )

        source_col = F.when(
            F.length(source_col) == 0,
            F.lit(None)
        ).otherwise(
            source_col
        )

    # Backblaze `date` is a DATE,
    # not a TIMESTAMP.
    if source == "date":

        return (
            F.to_date(
                source_col,
                "yyyy-MM-dd"
            )
            .alias(target)
        )

    if transformation == "to_date":

        return (
            F.to_date(
                source_col,
                "yyyy-MM-dd"
            )
            .alias(target)
        )

    if transformation == "to_timestamp":

        return (
            F.to_timestamp(
                source_col,
                "yyyy-MM-dd"
            )
            .alias(target)
        )

    return (
        source_col
        .cast(datatype)
        .alias(target)
    )


source_columns = set(
    df_bronze.columns
)


# ---------------------------------------------------------
# SCHEMA-LEVEL QUARANTINE
# ---------------------------------------------------------
missing_required_fields = [

    field["name"]

    for field in canonical_columns

    if not field["nullable"]

    and field.get(
        "source_column"
    )
    not in source_columns
]


if missing_required_fields:

    rejection_reason = (
        "missing_required_source_column:"
        +
        ",".join(
            missing_required_fields
        )
    )

    df_quarantine = (
        df_bronze

        .withColumn(
            "rejection_reason",
            F.lit(
                rejection_reason
            )
        )

        .withColumn(
            "rejected_at",
            F.current_timestamp()
        )

        .withColumn(
            "quarantine_processing_run_id",
            F.lit(run_id)
        )
    )

    if not spark.catalog.tableExists(
        quarantine_table
    ):

        (
            df_quarantine.writeTo(
                quarantine_table
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

            .tableProperty(
                "write.spark.accept-any-schema",
                "true"
            )

            .create()
        )

    else:

        (
            df_quarantine.writeTo(
                quarantine_table
            )
            .option(
                "mergeSchema",
                "true"
            )
            .append()
        )

    raise ValueError(
        "Required source columns are missing. "
        f"Records were quarantined in "
        f"{quarantine_table}. "
        f"Missing fields: "
        f"{missing_required_fields}"
    )


# ---------------------------------------------------------
# 5A. BUILD CANONICAL EXPRESSIONS
# ---------------------------------------------------------
canonical_expressions = [

    build_source_expression(
        field,
        source_columns
    )

    for field in canonical_columns
]


# ---------------------------------------------------------
# 5B. REQUIRED-FIELD ROW VALIDATION
# ---------------------------------------------------------
required_rejection_expressions = []


for field in canonical_columns:

    if field["nullable"]:
        continue

    target = field["name"]

    source = field.get(
        "source_column"
    )

    if source not in source_columns:
        continue

    source_col = F.col(
        source
    )

    if target in {
        "drive_serial_number",
        "drive_model"
    }:

        source_col = F.trim(
            source_col
        )

        required_invalid_condition = (
            source_col.isNull()
            |
            (source_col == "")
        )

    elif source == "date":

        parsed_date = F.to_date(
            source_col,
            "yyyy-MM-dd"
        )

        required_invalid_condition = (
            source_col.isNull()
            |
            (
                F.trim(source_col)
                == ""
            )
            |
            parsed_date.isNull()
        )

    else:

        parsed_value = (
            source_col.cast(
                field["datatype"]
            )
        )

        required_invalid_condition = (
            source_col.isNull()
            |
            (
                F.trim(source_col)
                == ""
            )
            |
            parsed_value.isNull()
        )

    required_rejection_expressions.append(

        F.when(
            required_invalid_condition,
            F.lit(
                f"invalid_required_field:{target}"
            )
        )

    )


if required_rejection_expressions:

    reasons_array = (
        F.array_compact(
            F.array(
                *required_rejection_expressions
            )
        )
    )

    invalid_condition = (
        F.size(
            reasons_array
        ) > 0
    )

    rejection_reason_expression = (
        F.when(
            invalid_condition,
            F.concat_ws(
                ";",
                reasons_array
            )
        )
        .otherwise(
            F.lit(None)
        )
    )

else:

    invalid_condition = F.lit(
        False
    )

    rejection_reason_expression = (
        F.lit(None)
    )


# ---------------------------------------------------------
# 5C. SINGLE CANONICAL CANDIDATE
# ---------------------------------------------------------
df_silver_candidate = (

    df_bronze

    .select(
        *canonical_expressions,
        "release_id",
        "source_file",
        "ingested_at",
        invalid_condition.alias(
            "_is_invalid"
        ),
        rejection_reason_expression.alias(
            "_rejection_reason"
        )
    )

    .persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# ---------------------------------------------------------
# 5D. WRITE REQUIRED-FIELD FAILURES TO QUARANTINE
# ---------------------------------------------------------
df_quarantine = (

    df_silver_candidate

    .filter(
        F.col("_is_invalid")
    )

    .withColumn(
        "rejection_reason",
        F.col("_rejection_reason")
    )

    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )

    .withColumn(
        "quarantine_processing_run_id",
        F.lit(run_id)
    )

    .drop(
        "_is_invalid",
        "_rejection_reason"
    )
)


quarantine_has_rows = (
    df_quarantine
    .limit(1)
    .count()
    > 0
)


if quarantine_has_rows:

    if not spark.catalog.tableExists(
        quarantine_table
    ):

        (
            df_quarantine.writeTo(
                quarantine_table
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

            .tableProperty(
                "write.spark.accept-any-schema",
                "true"
            )

            .create()
        )

    else:

        (
            df_quarantine.writeTo(
                quarantine_table
            )
            .option(
                "mergeSchema",
                "true"
            )
            .append()
        )

    print(
        "Quarantine records written."
    )

else:

    print(
        "No required-field quarantine records "
        "for this release."
    )


# ---------------------------------------------------------
# 6. REMOVE QUARANTINED RECORDS FROM SILVER INPUT
# ---------------------------------------------------------
df_silver_candidate = (

    df_silver_candidate

    .filter(
        ~F.col("_is_invalid")
    )

    .drop(
        "_is_invalid",
        "_rejection_reason"
    )
)


# ---------------------------------------------------------
# 7. RETAIN OPERATIONAL LINEAGE METADATA
# ---------------------------------------------------------
df_silver_candidate = (

    df_silver_candidate

    .withColumnRenamed(
        "ingested_at",
        "bronze_ingested_at"
    )

    .withColumn(
        "processing_run_id",
        F.lit(run_id)
    )

    .withColumn(
        "silver_processed_at",
        F.current_timestamp()
    )
)


# ---------------------------------------------------------
# 8. DETERMINISTIC DEDUPLICATION
# ---------------------------------------------------------
# Business observation key:
# one drive + one daily observation within one release.
#
# If the same key appears multiple times, keep the latest
# Bronze ingestion record deterministically.
# ---------------------------------------------------------
dedup_key = [

    "release_id",
    "snapshot_date",
    "drive_serial_number"
]


window_spec = (

    Window

    .partitionBy(
        *dedup_key
    )

    .orderBy(

        F.col(
            "bronze_ingested_at"
        ).desc(),

        F.col(
            "source_file"
        ).desc(),

        F.col(
            "processing_run_id"
        ).desc()
    )
)


df_silver_candidate = (

    df_silver_candidate

    .withColumn(
        "_row_number",
        F.row_number().over(
            window_spec
        )
    )

    .filter(
        F.col("_row_number") == 1
    )

    .drop(
        "_row_number"
    )
)


# ---------------------------------------------------------
# 9. CREATE OR MERGE SILVER ICEBERG TABLE
#
# IMPORTANT:
# THE EXISTING MERGE LOGIC IS PRESERVED.
# ---------------------------------------------------------
if not spark.catalog.tableExists(
    silver_table
):

    (
        df_silver_candidate.writeTo(
            silver_table
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
            F.col("release_id"),
            months("snapshot_date")
        )

        .create()
    )

    print(
        "Silver Iceberg table created."
    )

else:

    df_silver_candidate.createOrReplaceTempView(
        "silver_source"
    )

    formatted_release_id = (

        int(release_id)

        if str(
            release_id
        ).isdigit()

        else
        f"'{release_id}'"
    )


    merge_query = f"""

        MERGE INTO
            {silver_table}
        AS target

        USING
            silver_source
        AS source

        ON
            target.release_id =
                {formatted_release_id}

            AND target.release_id =
                source.release_id

            AND target.snapshot_date =
                source.snapshot_date

            AND target.drive_serial_number =
                source.drive_serial_number

        WHEN MATCHED THEN
            UPDATE SET *

        WHEN NOT MATCHED THEN
            INSERT *

    """


    spark.sql(
        merge_query
    )

    print(
        "Silver Iceberg MERGE completed."
    )


# Release persisted candidate
# after quarantine and Silver
# processing have consumed it.
df_silver_candidate.unpersist()


# ---------------------------------------------------------
# 10. BASIC VALIDATION
# ---------------------------------------------------------
#
# Incremental:
#     validate only the processed source file.
#
# Backfill:
#     validate the requested release.
# ---------------------------------------------------------

if input_path is not None:

    silver_count = (

        spark.table(
            silver_table
        )

        .filter(
            F.col("source_file")
            ==
            F.lit(input_path)
        )

        .count()
    )

else:

    silver_count = (

        spark.table(
            silver_table
        )

        .filter(
            F.col("release_id")
            ==
            F.lit(release_id)
        )

        .count()
    )


# ---------------------------------------------------------
# 11. COMMIT
# ---------------------------------------------------------
job.commit()


print(
    "============================================"
)

print(
    "SILVER JOB COMMITTED"
)

print(
    "============================================"
)