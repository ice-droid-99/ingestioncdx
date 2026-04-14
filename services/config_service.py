import json
from utils.s3_io import s3_read_text

class ConfigService:
    @staticmethod
    def load_config(path_or_s3: str) -> dict:
        if path_or_s3.startswith("s3://"):
            return json.loads(s3_read_text(path_or_s3))
        with open(path_or_s3, "r", encoding="utf-8") as f:
            return json.load(f)