import boto3
from botocore.exceptions import ClientError
from utils.configs import config

s3_client = boto3.client('s3', region_name=config.get('region'))


def create_bucket(bucket_name:str, region: str):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print("Bucket already exists. Skipping creation.")
        raise
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['404', 'NoSuchBucket']:
            try:
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print("Bucket created successfully.")
                return True
            except ClientError as create_error:
                print(str(create_error))
                raise
        else:
            print(str(e))
            raise

def create_bucket_prefixes(bucket_name:str, landing_prefix: str, report_prefix: str):
    """Create bucket prefixes if it doesn't exist."""
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=landing_prefix,
            Body=b''
        )
        print("Landing prefix(folder) created successfully.")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=report_prefix,
            Body=b''
        )
        print("Report prefix(folder) created successfully.")
        return True
    except ClientError as e:
        print(str(e))
        raise


def s3_init():
    bucket = config.get('bucket_name')
    landing_prefix_key = config.get('landing_prefix')
    report_prefix_key = config.get('report_prefix')
    aws_region = config.get('region')

    if not all([bucket, landing_prefix_key, report_prefix_key, aws_region]):
        print("Missing required environment variables")
        raise ValueError("Missing required environment variables")

    if create_bucket(bucket_name=bucket, region=aws_region):
        create_bucket_prefixes(
            bucket_name=bucket,
            landing_prefix=landing_prefix_key,
            report_prefix=report_prefix_key
        )
    print("Configured S3 bucket creation successfully.")
    return True