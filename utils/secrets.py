import os
import json
import boto3

def load_salesforce_credentials_from_secrets_manager():
    secret_id = os.getenv("SF_SECRET_ID", "").strip()
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-2"
    if not secret_id:
        return

    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_id)
    secret_str = resp.get("SecretString")
    if not secret_str:
        raise ValueError(f"Secret {secret_id} has no SecretString")

    payload = json.loads(secret_str)
    for k in ["SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN", "SF_DOMAIN"]:
        v = payload.get(k)
        if v and not os.getenv(k):
            os.environ[k] = str(v)