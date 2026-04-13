from utils.file_utils import read_json, write_json

class SchemaService:
    def __init__(self, schema_registry_path: str):
        self.schema_registry_path = schema_registry_path
        self.registry = read_json(schema_registry_path, default={})

    @staticmethod
    def _schema_signature(columns):
        return "|".join(sorted(columns))

    def get_version_and_update_if_needed(self, table: str, current_columns: list):
        current_sig = self._schema_signature(current_columns)
        table_info = self.registry.get(table)

        if not table_info:
            self.registry[table] = {"current_version": 1, "signature": current_sig}
            write_json(self.schema_registry_path, self.registry)
            return 1, True

        if table_info["signature"] != current_sig:
            table_info["current_version"] += 1
            table_info["signature"] = current_sig
            self.registry[table] = table_info
            write_json(self.schema_registry_path, self.registry)
            return table_info["current_version"], True

        return table_info["current_version"], False