import sys
import uuid

from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


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

release_id = args["release_id"]
run_id = str(uuid.uuid4())

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args["JOB_NAME"],
    args
)


# =========================================================
# 2. TABLE DEFINITIONS
# =========================================================

# ---------------------------------------------------------
# SOURCE
# ---------------------------------------------------------

silver_table = (
    "glue_catalog.backblaze_db.silver_drivestats"
)

# ---------------------------------------------------------
# NORMAL S3 ICEBERG TARGETS
# ---------------------------------------------------------

normal_model_gold_table = (
    "glue_catalog.backblaze_db."
    "gold_drive_model_metrics_s3"
)

normal_release_gold_table = (
    "glue_catalog.backblaze_db."
    "gold_release_metrics_s3"
)



# =========================================================
# 3. START LOG
# =========================================================

# print("============================================")
# print("BACKBLAZE GOLD JOB")
# print("============================================")
# print(f"Release ID        : {release_id}")
# print(f"Processing Run ID : {run_id}")
# print(f"Silver Source     : {silver_table}")
# print("============================================")


# =========================================================
# 4. READ ONLY THE REQUESTED RELEASE
# =========================================================

df_silver = (
    spark.table(silver_table)
    .filter(
        F.col("release_id") == F.lit(release_id)
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)


# =========================================================
# 5. FAIL FAST IF RELEASE DOES NOT EXIST
# =========================================================

silver_row_count = df_silver.count()

if silver_row_count == 0:

    df_silver.unpersist()

    raise ValueError(
        "No Silver rows found for "
        f"release_id={release_id}"
    )


print("============================================")
print("SILVER INPUT")
print("============================================")
print(f"Release ID : {release_id}")
print(f"Silver Rows: {silver_row_count}")
print("============================================")


# =========================================================
# 6. CRITICAL INPUT VALIDATION
# =========================================================
#
# Gold must not silently aggregate obviously invalid data.
#
# Required business fields:
#
#   drive_serial_number
#   drive_model
#   snapshot_date
#   failure_flag
# =========================================================

null_serial_count = (
    df_silver
    .filter(
        F.col("drive_serial_number").isNull()
    )
    .count()
)

null_model_count = (
    df_silver
    .filter(
        F.col("drive_model").isNull()
        |
        (F.trim(F.col("drive_model")) == "")
    )
    .count()
)

null_date_count = (
    df_silver
    .filter(
        F.col("snapshot_date").isNull()
    )
    .count()
)

invalid_failure_count = (
    df_silver
    .filter(
        F.col("failure_flag").isNull()
        |
        ~F.col("failure_flag").isin(0, 1)
    )
    .count()
)


if null_serial_count > 0:

    df_silver.unpersist()

    raise ValueError(
        "Gold input validation failed: "
        f"{null_serial_count} rows have null "
        "drive_serial_number."
    )


if null_model_count > 0:

    df_silver.unpersist()

    raise ValueError(
        "Gold input validation failed: "
        f"{null_model_count} rows have null/blank "
        "drive_model."
    )


if null_date_count > 0:

    df_silver.unpersist()

    raise ValueError(
        "Gold input validation failed: "
        f"{null_date_count} rows have null "
        "snapshot_date."
    )


if invalid_failure_count > 0:

    df_silver.unpersist()

    raise ValueError(
        "Gold input validation failed: "
        f"{invalid_failure_count} rows have invalid "
        "failure_flag values."
    )


# =========================================================
# 7. DETERMINE RELEASE DATE RANGE
# =========================================================

release_dates = (
    df_silver
    .agg(
        F.min("snapshot_date").alias("start_date"),
        F.max("snapshot_date").alias("end_date")
    )
    .collect()[0]
)

start_date = release_dates["start_date"]
end_date = release_dates["end_date"]

start_year = start_date.year
end_year = end_date.year


# =========================================================
# 8. VALIDATE RELEASE DOES NOT CROSS CALENDAR YEARS
# =========================================================
#
# Our AFR calculation uses the number of days in the
# calendar year.
#
# We therefore require a release to belong to one
# calendar year.
# =========================================================

if start_year != end_year:

    df_silver.unpersist()

    raise ValueError(
        "Gold AFR calculation requires a release to "
        "remain within one calendar year. "
        f"release_id={release_id} spans "
        f"{start_year} to {end_year}."
    )


# =========================================================
# 9. DETERMINE DAYS IN RELEASE YEAR
# =========================================================

is_leap_year = (
    start_year % 400 == 0
    or (
        start_year % 4 == 0
        and start_year % 100 != 0
    )
)

days_in_year = (
    366
    if is_leap_year
    else 365
)


print("============================================")
print("RELEASE CALENDAR")
print("============================================")
print(f"Start Date    : {start_date}")
print(f"End Date      : {end_date}")
print(f"Calendar Year : {start_year}")
print(f"Days in Year  : {days_in_year}")
print("============================================")


# =========================================================
# 10. RELEASE-LEVEL GOLD
# =========================================================
#
# Grain:
#
#     one row per release_id
#
# =========================================================

df_release_gold = (
    df_silver
    .groupBy("release_id")
    .agg(

        F.min(
            "snapshot_date"
        ).alias(
            "start_date"
        ),

        F.max(
            "snapshot_date"
        ).alias(
            "end_date"
        ),

        F.countDistinct(
            "snapshot_date"
        )
        .cast("long")
        .alias(
            "distinct_snapshot_dates"
        ),

        F.countDistinct(
            "drive_serial_number"
        )
        .cast("long")
        .alias(
            "distinct_drives"
        ),

        F.countDistinct(
            "drive_model"
        )
        .cast("long")
        .alias(
            "distinct_models"
        ),

        F.count("*")
        .cast("long")
        .alias(
            "total_drive_days"
        ),

        F.sum(
            "failure_flag"
        )
        .cast("long")
        .alias(
            "drive_failures"
        )
    )
)


# =========================================================
# 11. RELEASE AFR
# =========================================================

df_release_gold = (
    df_release_gold

    .withColumn(
        "annualized_failure_rate_pct",

        F.when(
            F.col("total_drive_days") > 0,

            (
                F.col("drive_failures")
                /
                (
                    F.col("total_drive_days")
                    / F.lit(float(days_in_year))
                )
                *
                F.lit(100.0)
            )
        )

        .otherwise(
            F.lit(None).cast("double")
        )
    )

    .withColumn(
        "processing_run_id",
        F.lit(run_id)
    )

    .withColumn(
        "gold_processed_at",
        F.current_timestamp()
    )
)


# =========================================================
# 12. FINAL RELEASE COLUMN ORDER
# =========================================================

df_release_gold = df_release_gold.select(

    "release_id",
    "start_date",
    "end_date",
    "distinct_snapshot_dates",
    "distinct_drives",
    "distinct_models",
    "total_drive_days",
    "drive_failures",
    "annualized_failure_rate_pct",
    "processing_run_id",
    "gold_processed_at"
)


# =========================================================
# 13. MODEL-LEVEL GOLD
# =========================================================
#
# Grain:
#
#     release_id + drive_model
#
# =========================================================

df_model_gold = (
    df_silver
    .groupBy(
        "release_id",
        "drive_model"
    )
    .agg(

        F.countDistinct(
            "drive_serial_number"
        )
        .cast("long")
        .alias(
            "distinct_drives"
        ),

        F.count("*")
        .cast("long")
        .alias(
            "total_drive_days"
        ),

        F.sum(
            "failure_flag"
        )
        .cast("long")
        .alias(
            "drive_failures"
        ),

        F.min(
            "snapshot_date"
        ).alias(
            "first_snapshot_date"
        ),

        F.max(
            "snapshot_date"
        ).alias(
            "last_snapshot_date"
        )
    )
)


# =========================================================
# 14. MODEL AFR
# =========================================================

df_model_gold = (
    df_model_gold

    .withColumn(
        "annualized_failure_rate_pct",

        F.when(
            F.col("total_drive_days") > 0,

            (
                F.col("drive_failures")
                /
                (
                    F.col("total_drive_days")
                    / F.lit(float(days_in_year))
                )
                *
                F.lit(100.0)
            )
        )

        .otherwise(
            F.lit(None).cast("double")
        )
    )

    .withColumn(
        "processing_run_id",
        F.lit(run_id)
    )

    .withColumn(
        "gold_processed_at",
        F.current_timestamp()
    )
)


# =========================================================
# 15. FINAL MODEL COLUMN ORDER
# =========================================================

df_model_gold = df_model_gold.select(

    "release_id",
    "drive_model",
    "distinct_drives",
    "total_drive_days",
    "drive_failures",
    "annualized_failure_rate_pct",
    "first_snapshot_date",
    "last_snapshot_date",
    "processing_run_id",
    "gold_processed_at"
)


# =========================================================
# 16. AGGREGATION VALIDATION
# =========================================================

model_row_count = (
    df_model_gold.count()
)

release_row_count = (
    df_release_gold.count()
)


if model_row_count == 0:

    df_silver.unpersist()

    raise ValueError(
        "Model Gold aggregation produced zero rows."
    )


if release_row_count != 1:

    df_silver.unpersist()

    raise ValueError(
        "Release Gold aggregation should produce "
        f"exactly one row, got {release_row_count}."
    )


# =========================================================
# 17. SILVER → MODEL GOLD RECONCILIATION
# =========================================================

silver_totals = (
    df_silver
    .agg(

        F.count("*")
        .cast("long")
        .alias(
            "silver_drive_days"
        ),

        F.sum(
            "failure_flag"
        )
        .cast("long")
        .alias(
            "silver_failures"
        )
    )
    .collect()[0]
)


model_totals = (
    df_model_gold
    .agg(

        F.sum(
            "total_drive_days"
        )
        .cast("long")
        .alias(
            "gold_drive_days"
        ),

        F.sum(
            "drive_failures"
        )
        .cast("long")
        .alias(
            "gold_failures"
        )
    )
    .collect()[0]
)


silver_drive_days = (
    silver_totals["silver_drive_days"]
)

silver_failures = (
    silver_totals["silver_failures"]
)

gold_drive_days = (
    model_totals["gold_drive_days"]
)

gold_failures = (
    model_totals["gold_failures"]
)


if silver_drive_days != gold_drive_days:

    df_silver.unpersist()

    raise RuntimeError(
        "Drive-day reconciliation failed. "
        f"Silver={silver_drive_days}; "
        f"Gold={gold_drive_days}"
    )


if silver_failures != gold_failures:

    df_silver.unpersist()

    raise RuntimeError(
        "Failure reconciliation failed. "
        f"Silver={silver_failures}; "
        f"Gold={gold_failures}"
    )


# =========================================================
# 18. RELEASE GOLD RECONCILIATION
# =========================================================

release_result = (
    df_release_gold
    .collect()[0]
)

if (
    release_result["total_drive_days"]
    !=
    silver_drive_days
):

    df_silver.unpersist()

    raise RuntimeError(
        "Release Gold drive-day reconciliation failed."
    )


if (
    release_result["drive_failures"]
    !=
    silver_failures
):

    df_silver.unpersist()

    raise RuntimeError(
        "Release Gold failure reconciliation failed."
    )


# =========================================================
# 19. NORMAL S3 MODEL GOLD
# =========================================================

if not spark.catalog.tableExists(
    normal_model_gold_table
):

    (
        df_model_gold.writeTo(
            normal_model_gold_table
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
        "Created normal S3 model Gold table."
    )

else:

    (
        df_model_gold.writeTo(
            normal_model_gold_table
        )
        .overwritePartitions()
    )

    print(
        "Overwrote normal S3 model Gold partition."
    )


# =========================================================
# 20. NORMAL S3 RELEASE GOLD
# =========================================================

if not spark.catalog.tableExists(
    normal_release_gold_table
):

    (
        df_release_gold.writeTo(
            normal_release_gold_table
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
        "Created normal S3 release Gold table."
    )

else:

    (
        df_release_gold.writeTo(
            normal_release_gold_table
        )
        .overwritePartitions()
    )

    print(
        "Overwrote normal S3 release Gold partition."
    )


# =========================================================
# 24. READ BACK NORMAL MODEL GOLD
# =========================================================

normal_model_count = (
    spark.table(
        normal_model_gold_table
    )
    .filter(
        F.col("release_id")
        ==
        F.lit(release_id)
    )
    .count()
)


# =========================================================
# 26. READ BACK NORMAL RELEASE GOLD
# =========================================================

normal_release_count = (
    spark.table(
        normal_release_gold_table
    )
    .filter(
        F.col("release_id")
        ==
        F.lit(release_id)
    )
    .count()
)


# =========================================================
# =========================================================
# 28. OUTPUT ROW COUNT VALIDATION
# =========================================================

if normal_model_count != model_row_count:

    df_silver.unpersist()

    raise RuntimeError(
        "Normal S3 model Gold validation failed. "
        f"Expected={model_row_count}; "
        f"Actual={normal_model_count}"
    )



if normal_release_count != 1:

    df_silver.unpersist()

    raise RuntimeError(
        "Normal S3 release Gold validation failed. "
        f"Expected=1; "
        f"Actual={normal_release_count}"
    )



# # =========================================================
# # 29. FINAL REPORT
# # =========================================================

# print("============================================")
# print("GOLD JOB VALIDATION")
# print("============================================")

# print(
#     f"Release ID              : {release_id}"
# )

# print(
#     f"Silver Rows             : {silver_row_count}"
# )

# print(
#     f"Model Gold Rows         : {model_row_count}"
# )

# print(
#     f"Normal S3 Model Rows    : {normal_model_count}"
# )
# print(
#     f"Release Gold Rows       : {release_row_count}"
# )

# print(
#     f"Normal S3 Release Rows  : {normal_release_count}"
# )

# print(
# )

# print(
#     f"Total Drive Days        : "
#     f"{release_result['total_drive_days']}"
# )

# print(
#     f"Drive Failures          : "
#     f"{release_result['drive_failures']}"
# )

# print(
#     f"AFR (%)                 : "
#     f"{release_result['annualized_failure_rate_pct']}"
# )

# print(
#     f"Days in Year            : "
#     f"{days_in_year}"
# )

# print(
#     "Silver → Gold           : PASS"
# )

# print(
#     "Normal S3 Write         : PASS"
# )
# print("============================================")


# =========================================================
# 30. RELEASE CACHE
# =========================================================

df_silver.unpersist()


# =========================================================
# 31. COMMIT
# =========================================================

job.commit()

print("============================================")
print("GOLD JOB COMPLETED SUCCESSFULLY")
print("============================================")