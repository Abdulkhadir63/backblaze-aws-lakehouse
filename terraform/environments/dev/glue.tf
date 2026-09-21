resource "aws_glue_job" "bronze_ingestion" {
  name     = "bronze_ingestion"
  role_arn = "arn:aws:iam::131912110087:role/Glue-S3-Access-Role"

  glue_version      = "6.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 480
  max_retries       = 0
  execution_class   = "FLEX"

  command {
    script_location = "s3://backblaze-de-lakehouse-project/glue_scripts/scripts/full-load/bronze/bronze_ingestion.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-glue-datacatalog"          = ""
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--datalake-formats"                 = "iceberg"
    "--TempDir"                          = "s3://backblaze-de-lakehouse-project/glue_scripts/temporary/"
    "--release_id"                       = "data_Q1_2026"
    "--enable-metrics"                   = ""
    "--spark-event-logs-path"            = "s3://aws-glue-assets-131912110087-ap-south-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "false"
    "--enable-observability-metrics"     = "true"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://backblaze-de-lakehouse-project/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
    "--enable-auto-scaling"              = "true"
  }

  lifecycle {
    ignore_changes = [
      command[0].script_location
    ]
  }
}

resource "aws_glue_job" "silver_cleaned" {
  name     = "silver_cleaned"
  role_arn = "arn:aws:iam::131912110087:role/Glue-S3-Access-Role"

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 480
  max_retries       = 0
  execution_class   = "STANDARD"

  command {
    script_location = "s3://backblaze-de-lakehouse-project/glue_scripts/scripts/full-load/silver/silver_cleaned.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-glue-datacatalog"          = ""
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--datalake-formats"                 = "iceberg"
    "--TempDir"                          = "s3://backblaze-de-lakehouse-project/glue_scripts/temporary/"
    "--release_id"                       = "data_Q1_2026"
    "--enable-metrics"                   = ""
    "--spark-event-logs-path"            = "s3://aws-glue-assets-131912110087-ap-south-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "true"
    "--additional-python-modules"        = "pyyaml"
    "--enable-observability-metrics"     = "true"
    "--conf"                             = "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://backblaze-de-lakehouse-project/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
    "--enable-auto-scaling"              = "true"
  }

  lifecycle {
    ignore_changes = [
      command[0].script_location
    ]
  }
}

resource "aws_glue_job" "data_quality_check" {
  name     = "data_quality_check"
  role_arn = "arn:aws:iam::131912110087:role/Glue-S3-Access-Role"

  glue_version      = "6.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 480
  max_retries       = 0
  execution_class   = "STANDARD"

  command {
    script_location = "s3://backblaze-de-lakehouse-project/glue_scripts/scripts/full-load/data_quality_check/data_quality_check.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-glue-datacatalog"          = ""
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--datalake-formats"                 = "iceberg"
    "--TempDir"                          = "s3://backblaze-de-lakehouse-project/glue_scripts/temporary/"
    "--release_id"                       = "data_Q1_2026"
    "--enable-metrics"                   = ""
    "--spark-event-logs-path"            = "s3://aws-glue-assets-131912110087-ap-south-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "true"
    "--additional-python-modules"        = "PyYAML"
    "--enable-observability-metrics"     = "true"
    "--conf"                             = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=file:///tmp/spark-warehouse"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
  }

  lifecycle {
    ignore_changes = [
      command[0].script_location
    ]
  }
}

resource "aws_glue_job" "gold_layer" {
  name     = "gold_layer"
  role_arn = "arn:aws:iam::131912110087:role/Glue-S3-Access-Role"

  glue_version      = "6.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 480
  max_retries       = 0
  execution_class   = "STANDARD"

  command {
    script_location = "s3://backblaze-de-lakehouse-project/glue_scripts/scripts/full-load/gold/gold_layer.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-metrics"                   = ""
    "--spark-event-logs-path"            = "s3://aws-glue-assets-131912110087-ap-south-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://backblaze-de-lakehouse-project/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.s3tables=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tables.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.s3tables.glue.id=131912110087:s3tablescatalog/backblaze-gold-analytics --conf spark.sql.catalog.s3tables.warehouse=s3://backblaze-gold-analytics/warehouse/"
    "--enable-glue-datacatalog"          = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--datalake-formats"                 = "iceberg"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://backblaze-de-lakehouse-project/glue_scripts/temporary/"
    "--release_id"                       = "data_Q1_2026"
  }

  lifecycle {
    ignore_changes = [
      command[0].script_location
    ]
  }
}

resource "aws_glue_job" "bronze_layer" {
  name     = "bronze_layer"
  role_arn = "arn:aws:iam::131912110087:role/Glue-S3-Access-Role"

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 480
  max_retries       = 0
  execution_class   = "STANDARD"

  command {
    script_location = "s3://backblaze-de-lakehouse-project/glue_scripts/scripts/incremental-load/bronze/bronze_layer.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-metrics"                   = ""
    "--spark-event-logs-path"            = "s3://aws-glue-assets-131912110087-ap-south-1/scripts/incremental-load/bronze/"
    "--enable-job-insights"              = "true"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://backblaze-de-lakehouse-project/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-glue-datacatalog"          = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--datalake-formats"                 = "iceberg"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://backblaze-de-lakehouse-project/glue_scripts/temporary/"
  }

  lifecycle {
    ignore_changes = [
      command[0].script_location
    ]
  }
}

resource "aws_glue_job" "silver_layer" {
  name     = "silver_layer"
  role_arn = "arn:aws:iam::131912110087:role/Glue-S3-Access-Role"

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 480
  max_retries       = 0
  execution_class   = "STANDARD"

  command {
    script_location = "s3://backblaze-de-lakehouse-project/glue_scripts/scripts/incremental-load/silver/silver_layer.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-metrics"                   = ""
    "--spark-event-logs-path"            = "s3://aws-glue-assets-131912110087-ap-south-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "true"
    "--additional-python-modules"        = "pyyaml"
    "--enable-observability-metrics"     = "true"
    "--conf"                             = "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://backblaze-de-lakehouse-project/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = ""
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--datalake-formats"                 = "iceberg"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://backblaze-de-lakehouse-project/glue_scripts/temporary/"
  }

  lifecycle {
    ignore_changes = [
      command[0].script_location
    ]
  }
}

resource "aws_glue_job" "data_quality_layer" {
  name     = "data_quality_layer"
  role_arn = "arn:aws:iam::131912110087:role/Glue-S3-Access-Role"

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 480
  max_retries       = 0
  execution_class   = "STANDARD"

  command {
    script_location = "s3://backblaze-de-lakehouse-project/glue_scripts/scripts/incremental-load/data_quality_check/data_quality_layer.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-metrics"                   = ""
    "--spark-event-logs-path"            = "s3://aws-glue-assets-131912110087-ap-south-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "true"
    "--additional-python-modules"        = "pyyaml"
    "--enable-observability-metrics"     = "true"
    "--conf"                             = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=file:///tmp/spark-warehouse"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = ""
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--datalake-formats"                 = "iceberg"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://backblaze-de-lakehouse-project/glue_scripts/temporary/"
  }

  lifecycle {
    ignore_changes = [
      command[0].script_location
    ]
  }
}

resource "aws_glue_job" "gold_analytics_layer" {
  name     = "gold_analytics_layer"
  role_arn = "arn:aws:iam::131912110087:role/Glue-S3-Access-Role"

  glue_version      = "6.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 480
  max_retries       = 0
  execution_class   = "STANDARD"

  command {
    script_location = "s3://backblaze-de-lakehouse-project/glue_scripts/scripts/incremental-load/gold/gold_analytics_layer.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-metrics"                   = ""
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://backblaze-de-lakehouse-project/glue_scripts/scripts/incremental-load/gold/"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://backblaze-de-lakehouse-project/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.s3tables=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tables.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.s3tables.glue.id=131912110087:s3tablescatalog/backblaze-gold-analytics --conf spark.sql.catalog.s3tables.warehouse=s3://backblaze-gold-analytics/warehouse/"
    "--enable-glue-datacatalog"          = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--datalake-formats"                 = "iceberg"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://backblaze-de-lakehouse-project/glue_scripts/temporary/"
  }

  lifecycle {
    ignore_changes = [
      command[0].script_location
    ]
  }
}
