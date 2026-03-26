import csv
import json
import logging
import os
from io import BytesIO, StringIO
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import date
from typing import Any
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sns_client = boto3.client('sns')

TOPIC_ARN = os.environ["TOPIC_ARN"]


def _publish_status(subject: str, message: dict[str, Any]) -> None:
    """Publish a JSON message to the configured SNS topic."""
    sns_client.publish(
        TopicArn=TOPIC_ARN,
        Subject=subject,
        Message=json.dumps(message, indent=2),
    )


def _parse_s3_event(event: dict[str, Any]) -> dict[str, Any]:
    """Construct the message to deliver"""
    records = event.get("Records", [])
    if not records:
        raise ValueError("No S3 events found")

    return {
        "bucket": records[0]["s3"]["bucket"]["name"],
        "key": records[0]["s3"]["object"]["key"],
        "event_name": records[0]["eventName"],
        "event_time": records[0]["eventTime"],
        "aws_region": records[0]["awsRegion"]
    }


def _publish_message(event: dict[str, Any]) -> dict[str, Any]:
    """Publish structured message to SNS."""

    logger.info("Received event: %s", json.dumps(event))

    try:
        parsed_event = _parse_s3_event(event)

        success_message = {
            "status": "SUCCESS",
            "message": "S3 event processed successfully.",
            "details": parsed_event,
        }

        _publish_status(subject="Success", message=success_message)
        logger.info("Success notification sent to SNS")

        return {
            "statusCode": 200,
            "body": success_message,
        }

    except Exception as exc:
        failure_message = {
            "status": "FAILED",
            "message": "S3 event processing failed.",
            "error": str(exc),
            "raw_event": event,
        }

        _publish_status(subject="Failed", message=failure_message)
        logger.exception("Failed to process S3 event")

        return {
            "statusCode": 500,
            "body": failure_message,
        }


def _publish_report_notification(bucket: str, output_key: str, source_key: str) -> None:
    """Publish an SNS notification after the report CSV has been written to S3."""
    try:
        report_message = {
            "status": "REPORT_GENERATED",
            "message": "Revenue summary report successfully written to S3.",
            "details": {
                "source_file": f"s3://{bucket}/{source_key}",
                "report_location": f"s3://{bucket}/{output_key}",
                "generated_at": str(date.today()),
            }
        }
        _publish_status(subject="Report Generated", message=report_message)
        logger.info("Report notification sent to SNS: s3://%s/%s", bucket, output_key)
    except Exception as e:
        logger.error("Failed to publish report notification: %s", e)
        raise


def _load_file_from_s3(bucket_name: str, key: str) -> StringIO:
    """Load S3 object into a StringIO text stream."""
    try:
        file = s3.get_object(Bucket=bucket_name, Key=key)
        return StringIO(file['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to load s3://{bucket_name}/{key}: {e}")
        raise


def _aggregate_revenue_by_city(csv_file: StringIO) -> defaultdict:
    """Aggregate total revenue per city from CSV stream."""
    city_revenue: defaultdict[str, Decimal] = defaultdict(Decimal)
    reader = csv.DictReader(csv_file)

    for i, row in enumerate(reader):
        try:
            city = row['city'].strip()
            revenue = Decimal(row['price'].strip()) * Decimal(row['quantity'].strip())
            city_revenue[city] += revenue
        except (InvalidOperation, KeyError) as e:
            logger.warning(f"Skipping row {i + 2}: {e} — {row}")

    return city_revenue


def _upload_summary(
        aggregate_data: defaultdict,
        bucket_name: str,
        output_key: str
) -> None:
    """Write revenue summary CSV and upload to S3."""
    try:
        output_buffer = BytesIO()
        output_buffer.write(b'city,total_revenue\n')
        for city, revenue in aggregate_data.items():
            output_buffer.write(f'{city},{revenue:.4f}\n'.encode('utf-8'))
        output_buffer.seek(0)
        s3.upload_fileobj(output_buffer, bucket_name, output_key)
    except Exception as e:
        logger.error(f"Failed to upload {output_key}: {e}")
        raise


def _execute_pipeline(bucket: str, key: str, output_key: str) -> None:
    """Execute the full pipeline."""
    _upload_summary(
        _aggregate_revenue_by_city(
            _load_file_from_s3(bucket, key)
        ),
        bucket,
        output_key
    )


def lambda_handler(event, context):
    bucket: str = event['Records'][0]['s3']['bucket']['name']
    key: str = event['Records'][0]['s3']['object']['key']

    _publish_message(event)

    try:
        file_date = key.split('/')[-1].split('.')[0].split('_')[-1]
    except IndexError:
        logger.warning(f"Could not extract date from key: {key}, using today")
        file_date = str(date.today())

    output_key = f'reports/city_revenue_summary_{file_date}.csv'

    try:
        _execute_pipeline(bucket=bucket, key=key, output_key=output_key)
        _publish_report_notification(bucket=bucket, output_key=output_key, source_key=key)
    except Exception as e:
        logger.exception(f"Pipeline failed for {key}: {e}")
        return {
            'statusCode': 500,
            'body': f'Pipeline failed for {key}: {e}'
        }

    logger.info(f"Processed: {key}, Output: {output_key}.")
    return {
        'statusCode': 200,
        'body': f'Successfully processed {key} → {output_key}'
    }