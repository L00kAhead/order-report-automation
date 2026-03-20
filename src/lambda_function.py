import csv
import logging
import boto3
from io import BytesIO, StringIO
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import date

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


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


def _execute_pipeline(bucket: str, key: str, output_key: str):
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

    try:
        file_date = key.split('/')[-1].split('.')[0].split('_')[-1]
    except IndexError:
        logger.warning(f"Could not extract date from key: {key}, using today")
        file_date = str(date.today())

    output_key = f'reports/city_revenue_summary_{file_date}.csv'

    _execute_pipeline(bucket=bucket, key=key, output_key=output_key)

    logger.info(f"Processed: {key}, Output: {output_key}.")
    return {
        'statusCode': 200,
        'body': f'Successfully processed {key} → {output_key}'
    }

