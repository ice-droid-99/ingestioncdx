from utils.s3_io import s3_read_json, s3_write_json

class SchemaService:
    def __init__(self, schema_registry_uri: str):
        if not schema_registry_uri.startswith("s3://"):
            raise ValueError("SchemaService expects an S3 URI (s3://...)")
        self.schema_registry_uri = schema_registry_uri
        self.registry = s3_read_json(schema_registry_uri, default={})

    @staticmethod
    def _schema_signature(columns):
        # minimal signature (names only). Later we can include types.
        return "|".join(sorted(columns))

    def get_version_and_update_if_needed(self, table: str, current_columns: list):
        current_sig = self._schema_signature(current_columns)
        table_info = self.registry.get(table)

        if not table_info:
            self.registry[table] = {"current_version": 1, "signature": current_sig}
            s3_write_json(self.schema_registry_uri, self.registry)
            return 1, True

        if table_info["signature"] != current_sig:
            table_info["current_version"] += 1
            table_info["signature"] = current_sig
            self.registry[table] = table_info
            s3_write_json(self.schema_registry_uri, self.registry)
            return table_info["current_version"], True

        return table_info["current_version"], False