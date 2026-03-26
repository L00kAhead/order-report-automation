import os
from dotenv import load_dotenv
load_dotenv()

config : dict[str, str | None] = {
    'bucket_name': os.getenv('BUCKET'),
    'landing_prefix': os.getenv('LANDING_PREFIX'),
    'report_prefix': os.getenv('REPORTING_PREFIX'),
    'region': os.getenv('AWS_REGION'),

    'lambda_function_name': os.getenv('LAMBDA_FUNCTION_NAME'),
    'lambda_role': os.getenv('LAMBDA_ROLE'),
    'lambda_timeout': os.getenv('LAMBDA_TIMEOUT'),
    'lambda_memory': os.getenv('LAMBDA_MEMORY'),
    'lambda_ephemeral_size': os.getenv('LAMBDA_EPHEMERAL_SIZE'),

    'codebuild_project_name': os.getenv('CODE_BUILD_PROJECT_NAME'),
    'codebuild_role_name': os.getenv('CODE_BUILD_ROLE_NAME'),
    'codebuild_source': os.getenv('CODE_BUILD_SOURCE'),
    'codebuild_repo_url': os.getenv('CODE_BUILD_REPO_URL'),
    'codebuild_environment': os.getenv('CODE_BUILD_ENVIRONMENT'),
    'codebuild_compute_type': os.getenv('CODE_BUILD_COMPUTE_TYPE'),
    'codebuild_image': os.getenv('CODE_BUILD_IMAGE'),
    'sns_topic_arn': os.getenv('SNS_TOPIC_ARN'),
}
