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
        self.config_uri = os.environ.get("CONFIG_URI", "s3://demo445/config/ingestion_config.json")
        self.output_prefix = os.environ.get("OUTPUT_PREFIX_URI", "s3://demo445/output/")
        self.schema_uri = os.environ.get("SCHEMA_URI", "s3://demo445/log/schema_registry.json")
        self.watermark_uri = os.environ.get("WATERMARK_URI", "s3://demo445/log/watermark_state.json")
        self.audit_table_prefix = os.environ.get("AUDIT_TABLE_PREFIX_URI", "s3://demo445/log/audit_table/")

        # Global run mode controlled by env / scheduler
        self.run_mode = os.environ.get("RUN_MODE", "incremental").strip().lower()
        if self.run_mode not in {"full", "incremental"}:
            raise ValueError("RUN_MODE must be one of: full, incremental")

        self.config = ConfigService.load_config(self.config_uri)

        self.sf_service = SalesforceService()
        self.schema_service = SchemaService(self.schema_uri)
        self.state_service = StateService(self.watermark_uri)
        self.audit_service = AuditService(self.audit_table_prefix)
        self.writer_service = WriterService(
            output_prefix_uri=self.output_prefix,
            output_format=os.getenv("DEFAULT_OUTPUT_FORMAT", "parquet")
        )

    def run(self):
        run_id = str(uuid.uuid4())
        tables = self.config.get("tables", [])
        logger.info(f"Starting run_id={run_id}, run_mode={self.run_mode}, tables={len(tables)}")

        for table_cfg in tables:
            if not table_cfg.get("active", True):
                continue

            table = table_cfg["table"]
            wm_col = table_cfg.get("watermark_column")
            extra_where = table_cfg.get("where_clause")

            start_ts = utc_now_iso()
            watermark_start = self.state_service.get_watermark(table)
            watermark_end = watermark_start
            schema_version = None
            schema_changed = False
            output_uri = ""
            error_message = ""
            status = "SUCCESS"
            row_count = 0

            try:
                # 1) metadata-driven schema
                described_columns = self.sf_service.describe_object_fields(table)

                if wm_col and wm_col not in described_columns:
                    raise ValueError(f"Configured watermark_column '{wm_col}' not found in {table}")

                # 2) schema versioning with history
                schema_version, schema_changed = self.schema_service.get_version_and_update_if_needed(
                    table=table,
                    current_columns=described_columns
                )
                if schema_changed:
                    logger.info(f"Schema changed for {table}. Using v{schema_version}")

                # 3) data fetch (chunked for wide objects)
                records = self.sf_service.query_all_chunked(
                    table=table,
                    columns=described_columns,
                    load_type=self.run_mode,
                    wm_col=wm_col,
                    last_watermark=watermark_start,
                    extra_where=extra_where,
                    max_query_length=int(os.getenv("SOQL_MAX_QUERY_LENGTH", "18000"))
                )
                row_count = len(records)

                # 4) write data + update watermark
                if row_count > 0:
                    output_uri = self.writer_service.write(
                        load_type=self.run_mode,
                        table=table,
                        version=schema_version,
                        run_id=run_id,
                        records=records
                    )

                    # update watermark for BOTH full and incremental
                    if wm_col:
                        wm_values = [r.get(wm_col) for r in records if r.get(wm_col) is not None]
                        if wm_values:
                            watermark_end = max(wm_values)
                            self.state_service.set_watermark(table, watermark_end)
                else:
                    logger.info(f"No rows for {table}; watermark unchanged.")

            except Exception as e:
                status = "FAILED"
                error_message = str(e)
                logger.exception(f"Ingestion failed for table={table}: {e}")

            end_ts = utc_now_iso()

            audit_event = {
                "run_id": run_id,
                "table": table,
                "run_mode": self.run_mode,
                "status": status,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "row_count": row_count,
                "watermark_start": watermark_start or "",
                "watermark_end": watermark_end or "",
                "schema_version": schema_version if schema_version is not None else "",
                "schema_changed": schema_changed,
                "output_uri": output_uri,
                "error_message": error_message
            }

            audit_uri = self.audit_service.write_event(audit_event)
            logger.info(f"Completed table={table} status={status} rows={row_count} audit={audit_uri}")

        logger.info(f"Run completed. run_id={run_id}")