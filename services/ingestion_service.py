import os
import uuid
from utils.logger import get_logger
from utils.datetime_utils import utc_now_iso
from services.config_service import ConfigService
from services.salesforce_service import SalesforceService
from services.schema_service import SchemaService
from services.state_service import StateService
from services.audit_service import AuditService
from services.writer_service import WriterService

logger = get_logger("ingestion_service")

class IngestionService:
    def __init__(self):
        # Must be set in ECS env vars
        self.config_uri = os.environ.get("CONFIG_URI", "s3://demo445/config/ingestion_config.json")
        self.output_prefix = os.environ.get("OUTPUT_PREFIX_URI", "s3://demo445/output/")
        self.log_prefix = os.environ.get("LOG_PREFIX_URI", "s3://demo445/log/audit/")
        self.schema_uri = os.environ.get("SCHEMA_URI", "s3://demo445/log/schema_registry.json")
        self.watermark_uri = os.environ.get("WATERMARK_URI", "s3://demo445/log/watermark_state.json")

        self.config = ConfigService.load_config(self.config_uri)

        self.sf_service = SalesforceService()
        self.schema_service = SchemaService(self.schema_uri)
        self.state_service = StateService(self.watermark_uri)
        self.audit_service = AuditService(self.log_prefix)
        self.writer_service = WriterService(
            output_prefix_uri=self.output_prefix,
            output_format=os.getenv("DEFAULT_OUTPUT_FORMAT", "parquet")
        )

    def _build_soql(self, table_cfg: dict, last_watermark: str | None):
        table = table_cfg["table"]
        columns = table_cfg["columns"]
        load_type = table_cfg["load_type"]
        wm_col = table_cfg.get("watermark_column")
        extra_where = table_cfg.get("where_clause")

        base = f"SELECT {', '.join(columns)} FROM {table}"
        conditions = []

        if load_type == "incremental" and wm_col and last_watermark:
            conditions.append(f"{wm_col} > {self._quote_soql(last_watermark)}")

        if extra_where:
            conditions.append(f"({extra_where})")

        if conditions:
            base += " WHERE " + " AND ".join(conditions)

        if wm_col:
            base += f" ORDER BY {wm_col}"

        return base

    @staticmethod
    def _quote_soql(value: str):
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"

    def run(self):
        run_id = str(uuid.uuid4())
        tables = self.config.get("tables", [])

        logger.info(f"Starting run_id={run_id}, tables={len(tables)}")

        for table_cfg in tables:
            if not table_cfg.get("active", True):
                continue

            table = table_cfg["table"]
            load_type = table_cfg["load_type"]
            wm_col = table_cfg.get("watermark_column")

            start_ts = utc_now_iso()
            watermark_start = self.state_service.get_watermark(table)
            watermark_end = watermark_start
            schema_version = None
            output_uri = ""
            error_message = ""
            status = "SUCCESS"
            row_count = 0
            audit_uri = ""

            try:
                soql = self._build_soql(table_cfg, watermark_start)
                records = self.sf_service.query_all(soql)
                row_count = len(records)

                if row_count == 0:
                    current_columns = table_cfg["columns"]
                else:
                    all_cols = set()
                    for r in records:
                        all_cols.update(r.keys())
                    current_columns = sorted(all_cols)

                schema_version, changed = self.schema_service.get_version_and_update_if_needed(
                    table=table,
                    current_columns=current_columns
                )
                if changed:
                    logger.info(f"Schema changed for {table}. Using v{schema_version}")

                if row_count > 0:
                    output_uri = self.writer_service.write(
                        load_type=load_type,
                        table=table,
                        version=schema_version,
                        run_id=run_id,
                        records=records
                    )

                    if load_type == "incremental" and wm_col:
                        wm_values = [r.get(wm_col) for r in records if r.get(wm_col) is not None]
                        if wm_values:
                            watermark_end = max(wm_values)
                            self.state_service.set_watermark(table, watermark_end)

            except Exception as e:
                status = "FAILED"
                error_message = str(e)
                logger.exception(f"Ingestion failed for table={table}: {e}")

            end_ts = utc_now_iso()
            audit_event = {
                "run_id": run_id,
                "table": table,
                "load_type": load_type,
                "status": status,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "row_count": row_count,
                "watermark_start": watermark_start or "",
                "watermark_end": watermark_end or "",
                "schema_version": schema_version if schema_version is not None else "",
                "output_uri": output_uri,
                "error_message": error_message
            }
            audit_uri = self.audit_service.write_event(run_id, table, audit_event)
            logger.info(f"Completed table={table} status={status} rows={row_count} audit={audit_uri}")

        logger.info(f"Run completed. run_id={run_id}")