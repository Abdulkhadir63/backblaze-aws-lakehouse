import json
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


# =========================================================
# 1. CONFIGURATION
# =========================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

EXPECTED_BUCKET = (
    "backblaze-de-lakehouse-project"
)

EXPECTED_PREFIX = (
    "raw/drivestats/"
)

CONTROL_TABLE_NAME = (
    "backblaze-dev-pipeline-control"
)

PIPELINE_CONTROL_ID = (
    "PIPELINE#BACKBLAZE"
)

PENDING_GSI_PK = (
    "BACKBLAZE#PENDING"
)

STATE_MACHINE_ARN = (
    "arn:aws:states:ap-south-1:131912110087:"
    "stateMachine:backblaze-dev-file-processing"
)


# =========================================================
# 2. AWS CLIENTS
# =========================================================

dynamodb = boto3.resource(
    "dynamodb"
)

control_table = dynamodb.Table(
    CONTROL_TABLE_NAME
)

sfn = boto3.client(
    "stepfunctions"
)


# =========================================================
# 3. SOURCE METADATA PARSING
# =========================================================

def parse_release_id(key: str) -> str:

    relative_path = key[
        len(EXPECTED_PREFIX):
    ]

    parts = relative_path.split("/")

    if len(parts) < 2:

        raise ValueError(
            f"Invalid Backblaze RAW object key: {key}"
        )

    release_id = parts[0]

    if not release_id:

        raise ValueError(
            f"Unable to determine release_id: {key}"
        )

    return release_id


def parse_source_date(key: str) -> str:

    filename = key.rsplit(
        "/",
        1
    )[-1]

    if not filename.endswith(
        ".csv"
    ):

        raise ValueError(
            f"Expected CSV source file: {key}"
        )

    source_date = filename[:-4]

    parts = source_date.split("-")

    if len(parts) != 3:

        raise ValueError(
            f"Invalid Backblaze date filename: "
            f"{filename}"
        )

    return source_date


# =========================================================
# 4. VALIDATE S3 EVENT
# =========================================================

def validate_s3_record(
    s3_record: dict
) -> dict:

    if "s3" not in s3_record:

        raise ValueError(
            "Missing 's3' object."
        )

    s3_data = s3_record["s3"]

    bucket_data = s3_data.get(
        "bucket",
        {}
    )

    object_data = s3_data.get(
        "object",
        {}
    )

    bucket = bucket_data.get(
        "name"
    )

    key = object_data.get(
        "key"
    )

    if not bucket:

        raise ValueError(
            "Missing bucket name."
        )

    if not key:

        raise ValueError(
            "Missing object key."
        )

    key = unquote_plus(
        key
    )

    if bucket != EXPECTED_BUCKET:

        raise ValueError(
            f"Unexpected bucket: {bucket}"
        )

    if not key.startswith(
        EXPECTED_PREFIX
    ):

        raise ValueError(
            f"Object outside allowed prefix: {key}"
        )

    size = object_data.get(
        "size"
    )

    etag = object_data.get(
        "eTag"
    )

    event_name = s3_record.get(
        "eventName"
    )

    event_time = s3_record.get(
        "eventTime"
    )

    release_id = parse_release_id(
        key
    )

    source_date = parse_source_date(
        key
    )

    source_file = (
        f"s3://{bucket}/{key}"
    )

    received_at = (
        datetime.now(
            timezone.utc
        )
        .isoformat()
        .replace(
            "+00:00",
            "Z"
        )
    )

    return {

        "source_file":
            source_file,

        "bucket":
            bucket,

        "key":
            key,

        "release_id":
            release_id,

        "source_date":
            source_date,

        "size_bytes":
            size,

        "etag":
            etag,

        "event_name":
            event_name,

        "event_time":
            event_time,

        "received_at":
            received_at
    }


# =========================================================
# 5. ENSURE PIPELINE STATE EXISTS
# =========================================================

def ensure_pipeline_state():

    item = {

        "control_id":
            PIPELINE_CONTROL_ID,

        "entity_type":
            "PIPELINE",

        "status":
            "IDLE"
    }

    try:

        control_table.put_item(

            Item=item,

            ConditionExpression=(
                "attribute_not_exists(control_id)"
            )
        )

        logger.info(
            "Pipeline control state created."
        )

        return "CREATED"

    except ClientError as exc:

        code = (
            exc.response
            .get("Error", {})
            .get("Code")
        )

        if (
            code
            ==
            "ConditionalCheckFailedException"
        ):

            return "EXISTS"

        raise


# =========================================================
# 6. REGISTER FILE
# =========================================================

def register_file(
    metadata: dict
):

    source_file = (
        metadata["source_file"]
    )

    control_id = (
        f"FILE#{source_file}"
    )

    received_at = (
        metadata["received_at"]
    )

    # -----------------------------------------------------
    # GSI SORT KEY
    #
    # Arrival timestamp first.
    # Source file second gives deterministic tie-breaking.
    # -----------------------------------------------------

    gsi_sk = (
        f"{received_at}#{source_file}"
    )

    item = {

        "control_id":
            control_id,

        "entity_type":
            "FILE",

        "source_file":
            source_file,

        "bucket":
            metadata["bucket"],

        "key":
            metadata["key"],

        "release_id":
            metadata["release_id"],

        "source_date":
            metadata["source_date"],

        "size_bytes":
            metadata["size_bytes"],

        "etag":
            metadata["etag"],

        "event_name":
            metadata["event_name"],

        "event_time":
            metadata["event_time"],

        "received_at":
            received_at,

        "status":
            "PENDING",

        # -------------------------------------------------
        # Pending-work GSI
        # -------------------------------------------------

        "gsi_pk":
            PENDING_GSI_PK,

        "gsi_sk":
            gsi_sk
    }

    try:

        control_table.put_item(

            Item=item,

            ConditionExpression=(
                "attribute_not_exists(control_id)"
            )
        )

        logger.info(
            "Registered NEW source file: %s",
            source_file
        )

        return "NEW"

    except ClientError as exc:

        code = (
            exc.response
            .get("Error", {})
            .get("Code")
        )

        if (
            code
            !=
            "ConditionalCheckFailedException"
        ):

            raise

        logger.warning(
            "Duplicate source event: %s",
            source_file
        )

        return "DUPLICATE"


# =========================================================
# 7. START STEP FUNCTIONS
# =========================================================

def start_step_function():

    response = sfn.start_execution(

        stateMachineArn=
            STATE_MACHINE_ARN,

        input=json.dumps({})
    )

    execution_arn = (
        response["executionArn"]
    )

    logger.info(
        "Started Step Functions execution: %s",
        execution_arn
    )

    return execution_arn


# =========================================================
# 8. LAMBDA HANDLER
# =========================================================

def lambda_handler(
    event,
    context
):

    logger.info(
        "Received SQS event."
    )

    records = event.get(
        "Records"
    )

    if not records:

        raise ValueError(
            "SQS event contains no Records."
        )

    pipeline_state = (
        ensure_pipeline_state()
    )

    processed_count = 0
    new_count = 0
    duplicate_count = 0

    for sqs_record in records:

        body = sqs_record.get(
            "body"
        )

        if not body:

            raise ValueError(
                "SQS record missing body."
            )

        try:

            s3_event = json.loads(
                body
            )

        except json.JSONDecodeError as exc:

            raise ValueError(
                "SQS body is invalid JSON."
            ) from exc

        s3_records = (
            s3_event.get(
                "Records"
            )
        )

        if not s3_records:

            raise ValueError(
                "S3 event contains no Records."
            )

        for s3_record in s3_records:

            metadata = (
                validate_s3_record(
                    s3_record
                )
            )

            result = register_file(
                metadata
            )

            processed_count += 1

            if result == "NEW":

                new_count += 1

            else:

                duplicate_count += 1

    logger.info(
        "Registration complete. "
        "pipeline_state=%s "
        "processed=%s "
        "new=%s "
        "duplicates=%s",
        pipeline_state,
        processed_count,
        new_count,
        duplicate_count
    )

    # -----------------------------------------------------
    # IMPORTANT:
    #
    # Start the worker even if the event was a duplicate.
    #
    # Why?
    # A previous Lambda invocation could have successfully
    # registered the file and then failed before starting
    # Step Functions.
    #
    # The Step Function itself checks DynamoDB for PENDING
    # work and safely does nothing if there is none.
    # -----------------------------------------------------

    execution_arn = (
        start_step_function()
    )

    return {

        "status":
            "SUCCESS",

        "pipeline_state":
            pipeline_state,

        "processed_count":
            processed_count,

        "new_count":
            new_count,

        "duplicate_count":
            duplicate_count,

        "step_function_execution_arn":
            execution_arn
    }