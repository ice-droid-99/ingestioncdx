import os
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest
from utils.logger import get_logger

logger = get_logger("salesforce_service")

class SalesforceService:
    def __init__(self):
        self.sf = Salesforce(
            username=os.getenv("SF_USERNAME"),
            password=os.getenv("SF_PASSWORD"),
            security_token=os.getenv("SF_SECURITY_TOKEN"),
            domain=os.getenv("SF_DOMAIN", "login")
        )

    def describe_object_fields(self, object_name: str) -> list[str]:
        obj = getattr(self.sf, object_name)
        meta = obj.describe()

        fields = []
        for f in meta.get("fields", []):
            # IMPORTANT: do not filter by f["queryable"] (often not present per field)
            if not f.get("deprecatedAndHidden", False):
                name = f.get("name")
                if name:
                    fields.append(name)

        fields = sorted(set(fields))
        if "Id" not in fields:
            fields.insert(0, "Id")

        logger.info(f"Describe {object_name}: {len(fields)} fields discovered")
        return fields

    def query_all(self, soql: str):
        logger.info(f"Running SOQL: {soql}")
        data = self.sf.query_all(soql)
        records = data.get("records", [])
        cleaned = []
        for r in records:
            r.pop("attributes", None)
            cleaned.append(r)
        return cleaned

    def query_all_chunked(
        self,
        table: str,
        columns: list[str],
        load_type: str,
        wm_col: str | None,
        last_watermark: str | None,
        extra_where: str | None,
        max_query_length: int = 18000
    ) -> list[dict]:
        cols = sorted(set(columns))
        if "Id" not in cols:
            cols.insert(0, "Id")

        conditions = []
        if load_type == "incremental" and wm_col and last_watermark:
            # SOQL datetime literal must not be quoted
            conditions.append(f"{wm_col} > {last_watermark}")
        if extra_where:
            conditions.append(f"({extra_where})")

        where_clause = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        order_clause = f" ORDER BY {wm_col}" if wm_col else ""

        # chunk by query length
        chunks = []
        current = []
        for c in cols:
            trial = current + [c]
            soql = f"SELECT {', '.join(trial)} FROM {table}{where_clause}{order_clause}"
            if len(soql) > max_query_length and current:
                chunks.append(current)
                current = [c]
            else:
                current = trial
        if current:
            chunks.append(current)

        # ensure Id in every chunk
        fixed = []
        for ch in chunks:
            if "Id" not in ch:
                ch = ["Id"] + ch
            seen = set()
            uniq = []
            for x in ch:
                if x not in seen:
                    uniq.append(x)
                    seen.add(x)
            fixed.append(uniq)

        logger.info(f"{table}: querying in {len(fixed)} chunk(s)")

        merged = {}  # Id -> row dict
        for i, ch in enumerate(fixed, start=1):
            soql = f"SELECT {', '.join(ch)} FROM {table}{where_clause}{order_clause}"
            try:
                logger.info(f"{table}: chunk {i}/{len(fixed)}")
                recs = self.query_all(soql)
            except SalesforceMalformedRequest as e:
                # Skip bad field chunks gracefully; keep ingestion moving
                logger.warning(f"{table}: skipping chunk {i} due to malformed request: {e}")
                continue

            for r in recs:
                rid = r.get("Id")
                if not rid:
                    continue
                if rid not in merged:
                    merged[rid] = {"Id": rid}
                merged[rid].update(r)

        return list(merged.values())