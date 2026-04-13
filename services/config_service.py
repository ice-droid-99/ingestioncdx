"""
import json

class ConfigService:
    @staticmethod
    def load_config(path: str) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
"""    

import os
from utils.s3_utils import read_json

class ConfigService:
    @staticmethod
    def load_config_from_s3():
        bucket = os.getenv("S3_BUCKET")
        key = os.getenv("CONFIG_KEY")
        return read_json(bucket, key, default={})