import csv
import os
from utils.file_utils import ensure_dir

class AuditService:
    HEADERS = [
        "run_id", "table", "load_type", "status",
        "start_ts", "end_ts", "row_count",
        "watermark_start", "watermark_end",
        "schema_version", "output_path", "error_message"
    ]

    def __init__(self, audit_csv_path: str):
        self.audit_csv_path = audit_csv_path
        ensure_dir(os.path.dirname(audit_csv_path))
        if not os.path.exists(audit_csv_path):
            with open(audit_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.HEADERS)
                writer.writeheader()

    def append(self, row: dict):
        with open(self.audit_csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.HEADERS)
            writer.writerow(row)