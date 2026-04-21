import io
import pandas as pd
from utils.datetime_utils import utc_today, utc_compact_ts
from utils.s3_io import s3_write_bytes


class AuditService:
    """
    Writes table-level audit events to S3 parquet dataset:
      s3://.../dt=YYYY-MM-DD/run_mode=<mode>/table=<table>/audit_<run_id>_<ts>.parquet
    """
    def __init__(self, audit_table_prefix_uri: str):
        if not audit_table_prefix_uri.startswith("s3://"):
            raise ValueError("AuditService expects an S3 prefix URI (s3://...)")
        self.audit_table_prefix_uri = audit_table_prefix_uri.rstrip("/") + "/"

    def write_event(self, event: dict) -> str:
        dt = utc_today()
        ts = utc_compact_ts()
        run_mode = str(event.get("run_mode", "unknown")).lower()
        table = str(event.get("table", "unknown"))
        run_id = str(event.get("run_id", "na"))

        uri = (
            f"{self.audit_table_prefix_uri}"
            f"dt={dt}/run_mode={run_mode}/table={table}/"
            f"audit_{run_id}_{ts}.parquet"
        )

        df = pd.DataFrame([event])
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        s3_write_bytes(uri, buf.getvalue(), content_type="application/octet-stream")
        return uri