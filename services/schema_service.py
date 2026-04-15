from utils.s3_io import s3_read_json, s3_write_json

class SchemaService:
    def __init__(self, schema_registry_uri: str):
        if not schema_registry_uri.startswith("s3://"):
            raise ValueError("SchemaService expects an S3 URI (s3://...)")
        self.schema_registry_uri = schema_registry_uri
        self.registry = s3_read_json(schema_registry_uri, default={})

    @staticmethod
    def _schema_signature(columns):
        return "|".join(sorted(columns))

    def get_version_and_update_if_needed(self, table: str, current_columns: list):
        current_cols_sorted = sorted(set(current_columns))
        current_sig = self._schema_signature(current_cols_sorted)
        table_info = self.registry.get(table)

        if not table_info:
            self.registry[table] = {
                "current_version": 1,
                "signature": current_sig,
                "columns": current_cols_sorted
            }
            s3_write_json(self.schema_registry_uri, self.registry)
            return 1, True

        if table_info.get("signature") != current_sig:
            new_version = int(table_info.get("current_version", 1)) + 1
            self.registry[table] = {
                "current_version": new_version,
                "signature": current_sig,
                "columns": current_cols_sorted
            }
            s3_write_json(self.schema_registry_uri, self.registry)
            return new_version, True

        return int(table_info.get("current_version", 1)), False