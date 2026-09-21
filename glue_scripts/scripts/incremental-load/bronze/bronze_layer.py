import sys
import uuid

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. INITIALIZATION & PARAMETERS
# ---------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
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


release_id = get_optional_arg(
    "release_id"
)

input_path = get_optional_arg(
    "input_path"
)


if release_id is None:

    raise ValueError(
        "Missing required argument: --release_id"
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

raw_s3_path = (
    f"s3://backblaze-de-lakehouse-project/"
    f"raw/drivestats/{release_id}/"
)


# ---------------------------------------------------------
# INCREMENTAL MODE
# ---------------------------------------------------------
# If --input_path is provided, process exactly that file.
# ---------------------------------------------------------
if input_path is not None:

    if not input_path.startswith(
        "s3://backblaze-de-lakehouse-project/"
        "raw/drivestats/"
    ):

        raise ValueError(
            "Invalid --input_path. "
            "Input must be under "
            "s3://backblaze-de-lakehouse-project/"
            "raw/drivestats/"
        )


    if not input_path.lower().endswith(".csv"):

        raise ValueError(
            "Invalid --input_path. "
            "Incremental input must be a CSV file."
        )


    if "*" in input_path:

        raise ValueError(
            "Wildcard input paths are not allowed "
            "for incremental processing."
        )


    raw_s3_path = input_path


target_table = (
    "glue_catalog.backblaze_db.bronze_drivestats"
)

metadata_columns = {
    "source_file",
    "release_id",
    "ingested_at",
    "processing_run_id"
}


# ---------------------------------------------------------
# 3. READ RAW
# ---------------------------------------------------------
# inferSchema=False is intentional:
# Bronze preserves incoming source fields as STRING.
# ---------------------------------------------------------
df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("inferSchema", "false")
    .csv(raw_s3_path)
)


# ---------------------------------------------------------
# 3A. SOURCE FILE LINEAGE
# ---------------------------------------------------------
# Incremental:
#     use the exact file passed by the event pipeline.
#
# Backfill:
#     discover source file using input_file_name().
# ---------------------------------------------------------
if input_path is not None:

    df_raw = (
        df_raw
        .withColumn(
            "source_file",
            F.lit(raw_s3_path).cast("string")
        )
    )

else:

    df_raw = (
        df_raw
        .withColumn(
            "source_file",
            F.input_file_name().cast("string")
        )
    )


# ---------------------------------------------------------
# 4. PRESERVE SOURCE COLUMNS AS STRING
# ---------------------------------------------------------
incoming_source_columns = [
    c for c in df_raw.columns
    if c != "source_file"
]

df_bronze = df_raw.select(
    *[
        F.col(c).cast("string").alias(c)
        for c in incoming_source_columns
    ],
    F.col("source_file")
)


# ---------------------------------------------------------
# 5. ADD BRONZE INGESTION METADATA
# ---------------------------------------------------------
df_bronze = (
    df_bronze
    .withColumn(
        "release_id",
        F.lit(release_id).cast("string")
    )
    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )
    .withColumn(
        "processing_run_id",
        F.lit(run_id).cast("string")
    )
)


# ---------------------------------------------------------
# 6. CREATE OR EVOLVE EXISTING ICEBERG TABLE
# ---------------------------------------------------------
table_exists = spark.catalog.tableExists(target_table)


# =========================================================
# CASE 1: FIRST CREATION
# =========================================================
if not table_exists:

    (
        df_bronze.writeTo(target_table)
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty(
            "write.parquet.compression-codec",
            "snappy"
        )
        .tableProperty(
            "write.spark.accept-any-schema",
            "true"
        )
        .partitionedBy(F.col("release_id"))
        .create()
    )

    print("============================================")
    print("ICEBERG BRONZE TABLE CREATED")
    print("============================================")
    print(f"Release ID       : {release_id}")
    print(f"Processing Run ID: {run_id}")
    print(f"Input Path       : {raw_s3_path}")
    print("Partition        : release_id")
    print("============================================")


# =========================================================
# CASE 2: EXISTING TABLE
# =========================================================
else:

    # -----------------------------------------------------
    # 6A. READ CURRENT ICEBERG TABLE SCHEMA
    # -----------------------------------------------------
    existing_schema = spark.table(target_table).schema

    existing_fields = {
        field.name: field
        for field in existing_schema.fields
    }

    existing_source_columns = [
        field.name
        for field in existing_schema.fields
        if field.name not in metadata_columns
    ]


    # -----------------------------------------------------
    # 6B. DETECT NEW SOURCE COLUMNS
    # -----------------------------------------------------
    new_columns = [
        c
        for c in incoming_source_columns
        if c not in existing_source_columns
    ]

    if new_columns:

        print("============================================")
        print("SCHEMA EVOLUTION DETECTED")
        print("============================================")

        for column_name in new_columns:

            print(
                f"Adding new column: "
                f"{column_name} STRING"
            )

            spark.sql(
                f"""
                ALTER TABLE {target_table}
                ADD COLUMN `{column_name}` STRING
                """
            )

    else:

        print("No new source columns detected.")


    # -----------------------------------------------------
    # 6C. REFRESH ICEBERG SCHEMA
    # -----------------------------------------------------
    updated_schema = spark.table(target_table).schema


    # -----------------------------------------------------
    # 6D. ALIGN DATAFRAME TO EXACT TABLE SCHEMA
    #
    # Existing incoming column:
    #     cast to target column type
    #
    # Missing incoming column:
    #     NULL cast to target column type
    #
    # This guarantees the DataFrame matches the table
    # schema before the Iceberg write.
    # -----------------------------------------------------
    incoming_columns = set(df_bronze.columns)

    aligned_expressions = []

    for field in updated_schema.fields:

        column_name = field.name

        if column_name in incoming_columns:

            aligned_expressions.append(
                F.col(column_name)
                .cast(field.dataType)
                .alias(column_name)
            )

        else:

            print(
                f"Incoming column missing: "
                f"{column_name} → NULL"
            )

            aligned_expressions.append(
                F.lit(None)
                .cast(field.dataType)
                .alias(column_name)
            )


    df_bronze_aligned = df_bronze.select(
        *aligned_expressions
    )


    # -----------------------------------------------------
    # 6E. WRITE USING EXISTING ICEBERG PARTITION SPEC
    #
    # IMPORTANT:
    # We do NOT call partitionedBy() here.
    #
    # The existing table already has:
    #     PARTITION BY release_id
    #
    # Iceberg will write according to that existing spec.
    # -----------------------------------------------------
    (
        df_bronze_aligned.writeTo(target_table)
        .append()
    )

    print("============================================")
    print("BRONZE DATA APPENDED")
    print("============================================")
    print(f"Release ID       : {release_id}")
    print(f"Processing Run ID: {run_id}")
    print(f"Input Path       : {raw_s3_path}")
    print(f"New Columns      : {new_columns}")
    print("============================================")


# ---------------------------------------------------------
# 7. COMMIT
# ---------------------------------------------------------
job.commit()

print("============================================")
print("BRONZE INGESTION COMPLETED")
print("============================================")
print(f"Release ID       : {release_id}")
print(f"Processing Run ID: {run_id}")
print(f"Input Path       : {raw_s3_path}")
print("============================================")