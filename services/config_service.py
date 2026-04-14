"""
import json

class ConfigService:
    @staticmethod
    def load_config(path: str) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
"""    


"""
import os
from utils.s3_io import read_json


class ConfigService:
    @staticmethod
    def load_config_from_s3():
        bucket = os.getenv("S3_BUCKET")
        key = os.getenv("CONFIG_KEY")
        return read_json(bucket, key, default={})
"""

import json
from utils.s3_io import s3_read_text

class ConfigService:
    @staticmethod
    def load_config(path_or_s3: str) -> dict:
        if path_or_s3.startswith("s3://"):
            return json.loads(s3_read_text(path_or_s3))
        with open(path_or_s3, "r", encoding="utf-8") as f:
            return json.load(f)