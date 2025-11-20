import boto3
import os
from botocore.exceptions import ClientError

secrets_envs = []

"""Recupera segredo do AWS Secrets Manager e atualiza as variáveis de ambiente."""
def get_secrets_and_update_secrets():
    client = _get_client()

    for secret_env in secrets_envs:
        os.environ[secret_env] = _get_secret(client, secret_env)
    

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
