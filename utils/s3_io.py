import json
import io
import boto3
import pandas as pd

s3 = boto3.client("s3")

def read_json(bucket, key, default=None):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return default

def write_json(bucket, key, data):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

def append_csv_row(bucket, key, headers, row):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing = obj["Body"].read().decode("utf-8")
    except Exception:
        existing = ",".join(headers) + "\n"

    vals = []
    for h in headers:
        v = str(row.get(h, "")).replace('"', '""')
        vals.append(f"\"{v}\"")
    existing += ",".join(vals) + "\n"

    s3.put_object(Bucket=bucket, Key=key, Body=existing.encode("utf-8"), ContentType="text/csv")

def write_parquet(bucket, key, records):
    df = pd.DataFrame(records)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())