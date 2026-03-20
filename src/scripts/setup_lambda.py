import json
import sys
import boto3
from botocore.exceptions import ClientError
from utils.configs import config
from utils.helper_functions import load_json_policy, build_lambda_package
import time

region = config.get('region') or 'us-east-1'
lambda_client = boto3.client('lambda', region_name=region)
iam_client = boto3.client('iam')
s3_client = boto3.client('s3', region_name=region)


def create_lambda_role(lambda_role: str, trust_policy: dict, description: str = ''):
    """Create Lambda execution role."""

    try:
        if iam_client.get_role(RoleName=lambda_role):
            raise Exception("Lambda execution role already exists. Create a new role or delete the existing one.")
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchEntity':
            raise
    try:
        response = iam_client.create_role(
            RoleName=lambda_role,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description=description,
        )
        print('Lambda execution role created')
        return response['Role']['Arn']
    except ClientError as e:
        print(e.response['Error']['Message'])
        return None

def create_lambda_policy():
    """Create Lambda execution policy with bucket permissions."""

    policy = load_json_policy('lambda_execution_policy.json')

    bucket_arn = f"arn:aws:s3:::{config['bucket_name']}"
    replacements = {
        '${S3_BUCKET_ARN}': bucket_arn,
        '${S3_LANDING_PREFIX}': config['landing_prefix'],
        '${S3_REPORTING_PREFIX}': config['report_prefix'],
    }

    policy_text = json.dumps(policy)
    for placeholder, value in replacements.items():
        policy_text = policy_text.replace(placeholder, value)

    return json.loads(policy_text)

def create_lambda_execution_role(lambda_role_name: str, description: str = ''):
    """Create Lambda execution role and attach policy."""

    trust_policy = load_json_policy('lambda_trust_policy.json')

    role_arn = create_lambda_role(
        lambda_role_name,
        trust_policy,
        description = description
    )

    if role_arn:
        execution_policy = create_lambda_policy()
        try:
            iam_client.put_role_policy(
                RoleName=lambda_role_name,
                PolicyName=f'{lambda_role_name}-inline-policy',
                PolicyDocument=json.dumps(execution_policy),
            )
            print('Lambda execution policy attached')
            return role_arn
        except ClientError as e:
            print(e.response['Error']['Message'])
            return None
    return None


def get_lambda_function_arn(lambda_func_name: str) -> str:
    """Return ARN of the Lambda function."""

    response = lambda_client.get_function(FunctionName=lambda_func_name)
    return response['Configuration']['FunctionArn']


def create_lambda_invoke_permission(lambda_func_name: str, bucket_name: str) -> bool:
    """Create invoke/execution permission for the Lambda function."""

    statement_id = f'{bucket_name}-s3-invoke'

    try:
        lambda_client.add_permission(
            FunctionName=lambda_func_name,
            StatementId=statement_id,
            Action='lambda:InvokeFunction',
            Principal='s3.amazonaws.com',
            SourceArn=f'arn:aws:s3:::{bucket_name}',
        )
        print('Lambda invoke permission added')
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            print('Lambda invoke permission already exists')
            return True
        print(e.response['Error']['Message'])
        return False


def configure_s3_event_notification(
        bucket_name: str,
        lambda_function_arn: str,
        landing_prefix: str,
) -> bool:
    """Configure S3 event notification to trigger Lambda on new CSV files in the landing prefix."""

    try:
        existing_notification = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)

        lambda_notifications = [
            notification
            for notification in existing_notification.get('LambdaFunctionConfigurations', [])
            if notification.get('LambdaFunctionArn') != lambda_function_arn
        ]

        lambda_notifications.append({
            'Id': f'{bucket_name}-landing-csv-notification',
            'LambdaFunctionArn': lambda_function_arn,
            'Events': ['s3:ObjectCreated:*'],
            'Filter': {
                'Key': {
                    'FilterRules': [
                        {
                            'Name': 'prefix',
                            'Value': landing_prefix,
                        },
                        {
                            'Name': 'suffix',
                            'Value': '.csv',
                        },
                    ]
                }
            },
        })

        notification_configuration = {}
        for key in ('TopicConfigurations', 'QueueConfigurations'):
            if existing_notification.get(key):
                notification_configuration[key] = existing_notification[key]
        notification_configuration['LambdaFunctionConfigurations'] = lambda_notifications

        s3_client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification_configuration,
        )
        print('S3 event notification configured')
        return True
    except ClientError as e:
        print(e.response['Error']['Message'])
        return False

def create_lambda(
        lambda_func_name: str,
        lambda_function_role_arn: str,
        memory:int,
        timeout:int,
        ephemeral_size: int,
        retries:int = 5
):
    """Create the Lambda function with the specified configuration."""

    try:
        lambda_client.get_function(FunctionName=lambda_func_name)
        raise Exception('Lambda function already exists. skipping creation.')
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceNotFoundException':
            raise

    try:
        lambda_client.create_function(
            Description="Generates city-wise revenue reports for orders.",
            FunctionName=lambda_func_name,
            Handler='lambda_function.lambda_handler',
            Role=lambda_function_role_arn,
            Runtime='python3.14',
            MemorySize=memory,
            Timeout=timeout,
            Architectures=['arm64'],
            EphemeralStorage={'Size': ephemeral_size},
            Code={'ZipFile': build_lambda_package()},
        )

        lambda_client.get_waiter('function_active').wait(
            FunctionName=lambda_func_name
        )
        print('Function created')
        return True
    except ClientError as e:
        error_code = e.response['Error'].get('Code')
        error_message = e.response['Error'].get('Message', '')

        role_not_ready = (
            error_code == 'InvalidParameterValueException'
            and 'cannot be assumed by Lambda' in error_message
        )

        if role_not_ready:
            if retries > 0:
                time.sleep(10)  # Wait before retrying
                print(f'Role not yet assumable by Lambda. {retries} attempt(s) remaining. Retrying...')
                return create_lambda(
                    lambda_func_name,
                    lambda_function_role_arn,
                    memory,
                    timeout,
                    ephemeral_size,
                    retries - 1
                )
            else:
                print('Max retries exceeded. Role still not assumable by Lambda.')
                return False

        print(error_message)
        return False


def create_s3_event_notification(
        lambda_func_name: str, 
        bucket_name: str, 
        landing_prefix: str
) -> bool:
    """Configure S3 event notification to trigger Lambda on new CSV files in the landing prefix."""

    bucket_name = bucket_name

    if not create_lambda_invoke_permission(lambda_func_name, bucket_name):
        return False

    try:
        lambda_function_arn = get_lambda_function_arn(lambda_func_name)
    except ClientError as e:
        print(e.response['Error']['Message'])
        return False

    return configure_s3_event_notification(
        bucket_name=bucket_name,
        lambda_function_arn=lambda_function_arn,
        landing_prefix=landing_prefix,
    )


def lambda_init():

    bucket_name = config.get('bucket_name')
    landing_prefix = config.get('landing_prefix')
    report_prefix = config.get('report_prefix')
    lambda_function_name = config.get('lambda_function_name')
    lambda_role = config.get('lambda_role')

    try:
        lambda_timeout = int(config.get('lambda_timeout'))
        lambda_memory = int(config.get('lambda_memory'))
        lambda_ephemeral_size = int(config.get('lambda_ephemeral_size'))
    except (TypeError, ValueError):
        raise TypeError('Invalid Lambda numeric configuration values')


    if not all([bucket_name, landing_prefix, report_prefix, lambda_function_name,
                lambda_role, lambda_timeout, lambda_memory, lambda_ephemeral_size]):
        print('Missing required environment variables')
        raise ValueError('Missing required environment variables')

    lambda_exec_role_arn = create_lambda_execution_role(
        lambda_role,
        description="Execution role for the order report Lambda function."
    )

    if not lambda_exec_role_arn:
        sys.exit(1)

    lambda_ready = create_lambda(
        lambda_func_name=lambda_function_name,
        lambda_function_role_arn=lambda_exec_role_arn,
        memory=lambda_memory,
        timeout=lambda_timeout,
        ephemeral_size=lambda_ephemeral_size,
    )

    if not lambda_ready:
        sys.exit(1)

    if not create_s3_event_notification(
        lambda_func_name=lambda_function_name,
        bucket_name=bucket_name,
        landing_prefix=landing_prefix
    ):
        sys.exit(1)


    config.update({
        'lambda_exec_role_arn': lambda_exec_role_arn,
        'lambda_func_arn': get_lambda_function_arn(lambda_function_name)
    })
    print('Configured lambda function successfully.')
    return True

