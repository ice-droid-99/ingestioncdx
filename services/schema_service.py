from utils.s3_io import s3_read_json, s3_write_json
from utils.datetime_utils import utc_now_iso

class SchemaService:
    def __init__(self, schema_registry_uri: str):
        if not schema_registry_uri.startswith("s3://"):
            raise ValueError("SchemaService expects an S3 URI (s3://...)")
        self.schema_registry_uri = schema_registry_uri
        self.registry = s3_read_json(schema_registry_uri, default={})

    @staticmethod
    def _schema_signature(columns):
        return "|".join(sorted(set(columns)))

    def _append_new_version(self, table: str, columns: list[str], signature: str):
        now = utc_now_iso()
        table_info = self.registry.get(table)

        if not table_info:
            table_info = {
                "current_version": 0,
                "current_signature": "",
                "versions": []
            }

        new_version_num = int(table_info.get("current_version", 0)) + 1
        version_entry = {
            "version": new_version_num,
            "signature": signature,
            "columns": sorted(set(columns)),
            "created_at": now
        }

        table_info["versions"].append(version_entry)
        table_info["current_version"] = new_version_num
        table_info["current_signature"] = signature
        self.registry[table] = table_info
        s3_write_json(self.schema_registry_uri, self.registry)
        return new_version_num

    def get_version_and_update_if_needed(self, table: str, current_columns: list):
        current_cols = sorted(set(current_columns))
        current_sig = self._schema_signature(current_cols)
        table_info = self.registry.get(table)

        # first time
        if not table_info:
            v = self._append_new_version(table, current_cols, current_sig)
            return v, True

        existing_sig = table_info.get("current_signature")
        if not existing_sig:
            # backward compatibility with old registry format
            existing_sig = table_info.get("signature")

        if existing_sig != current_sig:
            v = self._append_new_version(table, current_cols, current_sig)
            return v, True

        return int(table_info.get("current_version", 1)), False