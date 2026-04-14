import json
import boto3

s3 = boto3.client("s3")

def parse_s3_uri(s3_uri: str):
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Not an s3 uri: {s3_uri}")
    parts = s3_uri.replace("s3://", "", 1).split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key

def s3_read_text(s3_uri: str) -> str:
    bucket, key = parse_s3_uri(s3_uri)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")

def s3_write_text(s3_uri: str, text: str, content_type: str = "text/plain"):
    bucket, key = parse_s3_uri(s3_uri)
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type)

def s3_read_json(s3_uri: str, default):
    try:
        return json.loads(s3_read_text(s3_uri))
    except Exception:
        return default

def s3_write_json(s3_uri: str, data: dict):
    s3_write_text(s3_uri, json.dumps(data, indent=2), content_type="application/json")

def s3_write_bytes(s3_uri: str, b: bytes, content_type: str = "application/octet-stream"):
    bucket, key = parse_s3_uri(s3_uri)
    s3.put_object(Bucket=bucket, Key=key, Body=b, ContentType=content_type)

def s3_exists(s3_uri: str) -> bool:
    bucket, key = parse_s3_uri(s3_uri)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False