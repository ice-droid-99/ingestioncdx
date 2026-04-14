import io
import pandas as pd
from utils.datetime_utils import utc_compact_ts
from utils.s3_io import s3_write_bytes

class WriterService:
    def __init__(self, output_prefix_uri: str, output_format: str = "parquet"):
        if not output_prefix_uri.startswith("s3://"):
            raise ValueError("WriterService expects an S3 prefix URI (s3://...)")
        self.output_prefix_uri = output_prefix_uri.rstrip("/") + "/"
        self.output_format = output_format

    def write(self, load_type: str, table: str, version: int, run_id: str, records: list):
        load_ts = utc_compact_ts()
        key_prefix = f"{load_type}/{table}/v{version}/load_ts={load_ts}/"
        file_name = f"{table}_{run_id}"

        df = pd.DataFrame(records)

        if self.output_format == "csv":
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            uri = f"{self.output_prefix_uri}{key_prefix}{file_name}.csv"
            s3_write_bytes(uri, csv_bytes, content_type="text/csv")
            return uri

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        uri = f"{self.output_prefix_uri}{key_prefix}{file_name}.parquet"
        s3_write_bytes(uri, buf.getvalue(), content_type="application/octet-stream")
        return uri