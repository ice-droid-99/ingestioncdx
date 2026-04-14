import json
from utils.s3_io import s3_write_text
from utils.datetime_utils import utc_today

class AuditService:
    def __init__(self, audit_prefix_uri: str):
        if not audit_prefix_uri.startswith("s3://"):
            raise ValueError("AuditService expects an S3 prefix URI (s3://...)")
        self.audit_prefix_uri = audit_prefix_uri.rstrip("/") + "/"

    def write_event(self, run_id: str, table: str, event: dict):
        day = utc_today()
        uri = f"{self.audit_prefix_uri}dt={day}/{table}/{run_id}.json"
        s3_write_text(uri, json.dumps(event, indent=2), content_type="application/json")
        return uri