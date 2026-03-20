import json
import boto3
from botocore.exceptions import ClientError
from utils.configs import config
from utils.helper_functions import load_json_policy

region = config.get('region') or 'us-east-1'
cb_client = boto3.client('codebuild', region_name=region)
iam_client = boto3.client('iam')


def create_codebuild_role(role_name: str, trust_policy: dict, description: str = '') -> str:
    """Create CodeBuild service role, or raise if it already exists."""
    try:
        iam_client.get_role(RoleName=role_name)
        raise Exception('CodeBuild service role already exists. Delete it or use a different name.')
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchEntity':
            raise

    response = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description=description,
    )
    print('CodeBuild service role created.')
    return response['Role']['Arn']


def create_codebuild_policy(project_arn: str) -> dict:
    """Build CodeBuild execution policy, injecting ARNs from config."""
    
    lambda_func_arn = config.get('lambda_func_arn')
    lambda_exec_role_arn = config.get('lambda_exec_role_arn')

    if not lambda_func_arn or not lambda_exec_role_arn:
        raise ValueError(
            'lambda_func_arn and lambda_exec_role_arn must be set in config '
            'before CodeBuild policy can be built. Ensure lambda_init() ran successfully.'
        )

    policy = load_json_policy('code_build_execution_policy.json')
    replacements = {
        '${CODEBUILD_PROJECT_ARN}': project_arn,
        '${LAMBDA_FUNCTION_ARN}': lambda_func_arn,
        '${LAMBDA_EXECUTION_ROLE_ARN}': lambda_exec_role_arn,
    }
    policy_text = json.dumps(policy)
    for placeholder, value in replacements.items():
        policy_text = policy_text.replace(placeholder, value)
    return json.loads(policy_text)


def attach_codebuild_policy(project_arn: str, role_name: str) -> None:
    """Attach CodeBuild inline policy to the role. Raises on failure."""
    
    policy = create_codebuild_policy(project_arn)
    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName=f'{role_name}-inline-policy',
        PolicyDocument=json.dumps(policy),
    )
    print('CodeBuild policy attached.')


def create_codebuild_project(
    project_name: str,
    role_arn: str,
    source_type: str,
    source_location: str,
    image: str,
    environment: str,
    compute_type: str,
) -> None:
    """Create CodeBuild project, or skip if it already exists. Raises on failure."""
    existing = cb_client.batch_get_projects(names=[project_name])
    if existing['projects']:
        print('CodeBuild project already exists; skipping creation.')
        return

    cb_client.create_project(
        name=project_name,
        source={
            'type': source_type,
            'location': source_location,
            'gitCloneDepth': 1,
        },
        artifacts={'type': 'NO_ARTIFACTS'},
        environment={
            'type': environment,
            'image': image,
            'computeType': compute_type,
            'environmentVariables': [
                {'name': 'AWS_REGION', 'value': region},
            ],
        },
        serviceRole=role_arn,
        logsConfig={
            'cloudWatchLogs': {
                'status': 'ENABLED',
                'groupName': f'/aws/codebuild/{project_name}',
            },
        },
    )
    print('CodeBuild project created.')


def codebuild_init() -> bool:
    
    codebuild_project_name = config.get('codebuild_project_name')
    codebuild_role_name = config.get('codebuild_role_name')
    codebuild_source = config.get('codebuild_source')
    codebuild_repo_url = config.get('codebuild_repo_url')
    codebuild_image = config.get('codebuild_image')
    codebuild_environment = config.get('codebuild_environment')
    codebuild_compute_target = config.get('codebuild_compute_target')

    if not all([codebuild_project_name, codebuild_role_name, codebuild_source,
                codebuild_repo_url, codebuild_image, codebuild_environment, codebuild_compute_target]):
        raise ValueError('Missing required CodeBuild config values.')

    trust_policy = load_json_policy('codebuild_trust_policy.json')

    try:
        role_arn = create_codebuild_role(
            codebuild_role_name,
            trust_policy,
            description="Service role for CodeBuild to automate order report generation.",
        )

        create_codebuild_project(
            project_name=codebuild_project_name,
            role_arn=role_arn,
            source_type=codebuild_source,
            source_location=codebuild_repo_url,
            image=codebuild_image,
            environment=codebuild_environment,
            compute_type=codebuild_compute_target,
        )

        project_arn = cb_client.batch_get_projects(
            names=[codebuild_project_name]
        )['projects'][0]['arn']
        attach_codebuild_policy(project_arn=project_arn, role_name=codebuild_role_name)

    except (ClientError, ValueError) as exc:
        print(f'CodeBuild setup failed: {exc}')
        return False

    print('CodeBuild setup completed successfully.')
    return True