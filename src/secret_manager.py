import boto3
import os
from botocore.exceptions import ClientError

secrets_envs = {
    "PARTNER_TOKEN": "quanterra/prod/partner/api_key",
    "DB_HOST": "host",
    "DB_PORT": "port",
    "DB_NAME": "db",
    "DB_USER": "user",
    "DB_PASS": "password"
}

"""Update secrets of AWS Secrets Manager in local."""
def update_secrets():
    client = _get_client()

    for env_var, secret_name in secrets_envs.items():
        os.environ[env_var] = _get_secret(client, secret_name)

def _get_secret(client, secret_name):
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return secret
    except ClientError as e:
        raise e


def _get_client():
    session = boto3.session.Session()
    return session.client(
        service_name='secretsmanager',
        region_name= os.getenv('AWS_REGION')
    )
