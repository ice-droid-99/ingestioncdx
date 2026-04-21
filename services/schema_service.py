from utils.s3_io import s3_read_json, s3_write_json
from utils.datetime_utils import utc_now_iso


class SchemaService:
    """
    Schema registry with full history per table.
    Registry shape:
    {
      "Account": {
        "current_version": 2,
        "current_signature": "...",
        "versions": [
          {"version":1, "signature":"...", "columns":[...], "created_at":"..."},
          {"version":2, "signature":"...", "columns":[...], "created_at":"..."}
        ]
      }
    }
    """
    def __init__(self, schema_registry_uri: str):
        if not schema_registry_uri.startswith("s3://"):
            raise ValueError("SchemaService expects an S3 URI (s3://...)")
        self.schema_registry_uri = schema_registry_uri
        self.registry = s3_read_json(schema_registry_uri, default={})

    @staticmethod
    def _schema_signature(columns: list[str]) -> str:
        return "|".join(sorted(set(columns)))

    def _normalize_legacy_if_needed(self, table: str):
        """
        Backward compatibility with older flat format:
        {"current_version":1,"signature":"...","columns":[...]}
        """
        t = self.registry.get(table)
        if not t:
            return

        if "versions" in t and "current_signature" in t:
            return

        current_version = int(t.get("current_version", 1))
        current_signature = t.get("current_signature") or t.get("signature", "")
        current_columns = t.get("columns", [])

        self.registry[table] = {
            "current_version": current_version,
            "current_signature": current_signature,
            "versions": [
                {
                    "version": current_version,
                    "signature": current_signature,
                    "columns": sorted(set(current_columns)),
                    "created_at": utc_now_iso()
                }
            ]
        }

    def _append_new_version(self, table: str, columns: list[str], signature: str) -> int:
        now = utc_now_iso()
        table_info = self.registry.get(table)

        if not table_info:
            table_info = {
                "current_version": 0,
                "current_signature": "",
                "versions": []
            }

        new_version = int(table_info.get("current_version", 0)) + 1
        table_info["versions"].append({
            "version": new_version,
            "signature": signature,
            "columns": sorted(set(columns)),
            "created_at": now
        })
        table_info["current_version"] = new_version
        table_info["current_signature"] = signature

        self.registry[table] = table_info
        s3_write_json(self.schema_registry_uri, self.registry)
        return new_version

    def get_version_and_update_if_needed(self, table: str, current_columns: list[str]):
        current_cols = sorted(set(current_columns))
        current_sig = self._schema_signature(current_cols)

        table_info = self.registry.get(table)
        if not table_info:
            version = self._append_new_version(table, current_cols, current_sig)
            return version, True

        # normalize legacy structure if exists
        self._normalize_legacy_if_needed(table)
        table_info = self.registry.get(table)

        existing_sig = table_info.get("current_signature", "")
        if existing_sig != current_sig:
            version = self._append_new_version(table, current_cols, current_sig)
            return version, True

        return int(table_info.get("current_version", 1)), False