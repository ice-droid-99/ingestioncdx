import os
import pandas as pd
from utils.file_utils import ensure_dir
from utils.datetime_utils import utc_today

class WriterService:
    def __init__(self, output_root: str, output_format: str = "parquet"):
        self.output_root = output_root
        self.output_format = output_format

    def write(self, load_type: str, table: str, version: int, run_id: str, records: list):
        date_part = utc_today()
        base_path = os.path.join(
            self.output_root, load_type, table, f"v{version}", f"load_date={date_part}"
        )
        ensure_dir(base_path)

        file_name = f"{table}_{run_id}"
        df = pd.DataFrame(records)

        if self.output_format == "csv":
            out_path = os.path.join(base_path, f"{file_name}.csv")
            df.to_csv(out_path, index=False)
        else:
            out_path = os.path.join(base_path, f"{file_name}.parquet")
            df.to_parquet(out_path, index=False)

        return out_path