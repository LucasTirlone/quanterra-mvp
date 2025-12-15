import boto3
import os
from botocore.exceptions import ClientError

DB_SECRET_ID = os.getenv("DB_SECRET_ID", "/mvp/db/credentials")

secrets_envs = {
    "PARTNER_TOKEN": "quanterra/prod/partner/api_key",
}

"""Update secrets of AWS Secrets Manager in local."""
def update_secrets():
    client = _get_client()

    for env_var, secret_name in secrets_envs.items():
        os.environ[env_var] = _get_secret(client, secret_name)

    import json
    raw = _get_secret(client, DB_SECRET_ID)
    cfg = json.loads(raw)
    os.environ["DB_HOST"] = cfg["host"]
    os.environ["DB_PORT"] = str(cfg["port"])
    os.environ["DB_NAME"] = cfg["db"]
    os.environ["DB_USER"] = cfg["user"]
    os.environ["DB_PASS"] = cfg["password"]

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
