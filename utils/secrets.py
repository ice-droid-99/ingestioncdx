import os
import json
import boto3
from botocore.exceptions import ClientError


def load_salesforce_credentials_from_secrets_manager():
    """
    Loads SF creds from AWS Secrets Manager secret JSON:
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, SF_DOMAIN
    """
    secret_id = os.getenv("SF_SECRET_ID", "").strip()
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-2"

    if not secret_id:
        return False  # no secret configured

    client = boto3.client("secretsmanager", region_name=region)
    try:
        resp = client.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        raise RuntimeError(f"Failed to read SF secret '{secret_id}': {e}")

    secret_str = resp.get("SecretString")
    if not secret_str:
        raise RuntimeError(f"Secret '{secret_id}' has no SecretString")

    payload = json.loads(secret_str)

    for k in ["SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN", "SF_DOMAIN"]:
        v = payload.get(k)
        if v:
            os.environ[k] = str(v)

    return True