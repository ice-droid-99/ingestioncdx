import os
import re
import io
import time
import boto3
import pyarrow.parquet as pq

s3     = boto3.client("s3")
athena = boto3.client("athena")
glue   = boto3.client("glue")

BUCKET        = os.getenv("DATA_BUCKET",     "demo445")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX",   "output/")
DB            = os.getenv("GLUE_DB",         "ingestion_ext")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT",   "s3://demo445/athena-results/")
WORKGROUP     = os.getenv("ATHENA_WORKGROUP","primary")

FULL_RE = re.compile(r"^output/full/([^/]+)/v(\d+)/load_ts=([^/]+)/")
INC_RE  = re.compile(r"^output/incremental/([^/]+)/v(\d+)/load_ts=([^/]+)/")


# ── Type mapping ──────────────────────────────────────────────────────────────

def pa_type_to_athena(pa_type) -> str:
    """
    Convert pyarrow type to Athena DDL type.
    - struct        → skip (caller handles)
    - null          → string  (no data yet, safe fallback)
    - bool          → boolean
    - int32         → int
    - int64         → bigint
    - double/float  → double
    - date          → date
    - timestamp     → timestamp
    - decimal       → decimal(p,s)
    - everything else → string
    """
    s = str(pa_type)
    if s == "null":                          return "string"
    if s == "bool":                          return "boolean"
    if s in ("int8", "int16", "int32"):      return "int"
    if s in ("int64", "uint32", "uint64"):   return "bigint"
    if s in ("uint8", "uint16"):             return "int"
    if s in ("float", "float32"):            return "float"
    if s in ("double", "float64"):           return "double"
    if s in ("string", "utf8", "large_utf8","large_string"): return "string"
    if s == "binary":                        return "binary"
    if s in ("date32", "date64"):            return "date"
    if s.startswith("timestamp"):            return "timestamp"
    if s.startswith("decimal"):
        m = re.match(r"decimal128\((\d+),\s*(\d+)\)", s)
        return f"decimal({m.group(1)},{m.group(2)})" if m else "double"
    # struct / list / map / anything complex → caller skips these
    return None


# ── Schema reading from actual Parquet file ───────────────────────────────────

def read_schema_from_parquet(bucket: str, key: str) -> dict:
    """
    Download parquet file, read its Arrow schema,
    return { column_name_lower: athena_type }
    Skips structs, lists, maps entirely (they crash Athena Hive SerDe).
    null-typed columns become string.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    arrow_schema = pq.ParquetFile(buf).schema_arrow

    result = {}
    for field in arrow_schema:
        name     = field.name.lower()
        pa_type  = field.type

        # Skip struct / list / map — these are compound Salesforce fields
        # (BillingAddress, ShippingAddress etc.) — their sub-fields already
        # exist as flat columns in the same file
        type_str = str(pa_type)
        if type_str.startswith(("struct", "list<", "map<")):
            print(f"    skip struct/list/map: {field.name}")
            continue

        athena_type = pa_type_to_athena(pa_type)
        if athena_type is None:
            print(f"    skip unknown type {pa_type}: {field.name}")
            continue

        result[name] = athena_type

    return result


def find_first_parquet(prefix: str) -> str | None:
    """Return the S3 key of the first .parquet file under prefix."""
    pager = s3.get_paginator("list_objects_v2")
    for page in pager.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                return obj["Key"]
    return None


# ── Athena DDL helpers ────────────────────────────────────────────────────────

def run_sql(sql: str) -> str:
    """Fire an Athena DDL, wait for SUCCEEDED, raise on FAILED/CANCELLED."""
    print(f"  DDL: {sql[:130].strip()}")
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]

    for _ in range(100):           # 100 × 3 s = 5 min max per query
        time.sleep(3)
        resp  = athena.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return qid
        if state in ("FAILED", "CANCELLED"):
            reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena {state}: {reason}")

    raise TimeoutError(f"Athena timed out on: {sql[:80]}")


def table_exists(name: str) -> bool:
    try:
        glue.get_table(DatabaseName=DB, Name=name)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


def schema_to_ddl(schema: dict) -> str:
    return ",\n".join(f"  `{col}` {typ}" for col, typ in schema.items())


# ── S3 discovery ──────────────────────────────────────────────────────────────

def discover():
    """
    Scan output/ and return:
      full_latest  : { (table, version) → latest_load_ts }
      inc_versions : { (table, version) }
    """
    full_latest  = {}
    inc_versions = set()

    pager = s3.get_paginator("list_objects_v2")
    for page in pager.paginate(Bucket=BUCKET, Prefix=OUTPUT_PREFIX):
        for obj in page.get("Contents", []):
            k = obj["Key"]

            m = FULL_RE.match(k)
            if m:
                t, v, ts = m.group(1), m.group(2), m.group(3)
                key = (t, v)
                if key not in full_latest or ts > full_latest[key]:
                    full_latest[key] = ts

            m = INC_RE.match(k)
            if m:
                inc_versions.add((m.group(1), m.group(2)))

    return full_latest, inc_versions


# ── Table upsert logic ────────────────────────────────────────────────────────

def upsert_full(table: str, version: str, load_ts: str):
    """
    Full load table always points to the LATEST load_ts folder only.
    - First run  → read schema from parquet → CREATE EXTERNAL TABLE
    - Later runs → ALTER TABLE SET LOCATION to new load_ts
    """
    tbl = f"{table.lower()}_full_v{version}"
    loc = f"s3://{BUCKET}/output/full/{table}/v{version}/load_ts={load_ts}/"

    if table_exists(tbl):
        # Table already exists — just move the pointer to new timestamp
        run_sql(f"ALTER TABLE `{tbl}` SET LOCATION '{loc}'")
        print(f"  ✅ full updated : {tbl}  →  load_ts={load_ts}")
        return

    # First time — read schema from actual parquet file
    sample_key = find_first_parquet(
        f"output/full/{table}/v{version}/load_ts={load_ts}/"
    )
    if not sample_key:
        raise FileNotFoundError(f"No parquet under {loc}")

    print(f"  Reading schema from: {sample_key}")
    schema = read_schema_from_parquet(BUCKET, sample_key)
    print(f"  Columns ({len(schema)}): {list(schema.keys())[:5]} ...")

    run_sql(f"DROP TABLE IF EXISTS `{tbl}`")
    run_sql(f"""CREATE EXTERNAL TABLE `{tbl}` (
{schema_to_ddl(schema)}
)
STORED AS PARQUET
LOCATION '{loc}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')""")
    print(f"  ✅ full created  : {tbl}  ({len(schema)} cols)  →  load_ts={load_ts}")


def upsert_inc(table: str, version: str):
    """
    Incremental table points to the entire version folder.
    Partitioned by load_ts — MSCK REPAIR picks up every new batch.
    - First run  → read schema → CREATE EXTERNAL TABLE PARTITIONED BY load_ts
    - Later runs → MSCK REPAIR to register new partitions
    """
    tbl = f"{table.lower()}_inc_v{version}"
    loc = f"s3://{BUCKET}/output/incremental/{table}/v{version}/"

    if not table_exists(tbl):
        # Find any parquet file anywhere under this version to read schema
        sample_key = find_first_parquet(
            f"output/incremental/{table}/v{version}/"
        )
        if not sample_key:
            print(f"  ⚠️  no parquet found under {loc} — skipping")
            return

        print(f"  Reading schema from: {sample_key}")
        schema = read_schema_from_parquet(BUCKET, sample_key)
        print(f"  Columns ({len(schema)}): {list(schema.keys())[:5]} ...")

        run_sql(f"DROP TABLE IF EXISTS `{tbl}`")
        run_sql(f"""CREATE EXTERNAL TABLE `{tbl}` (
{schema_to_ddl(schema)}
)
PARTITIONED BY (`load_ts` string)
STORED AS PARQUET
LOCATION '{loc}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')""")
        print(f"  ✅ inc created   : {tbl}  ({len(schema)} cols)")

    # Always repair — registers any new load_ts= partitions
    run_sql(f"MSCK REPAIR TABLE `{tbl}`")
    print(f"  ✅ inc refreshed : {tbl}")


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    # Guard: only proceed if ECS task exited 0
    containers = event.get("detail", {}).get("containers", [])
    if containers and any(c.get("exitCode", 1) != 0 for c in containers):
        print("ECS task exited non-zero — skipping table refresh")
        return {"status": "skipped"}

    print("=" * 60)
    print("TABLE REFRESH START")
    print("=" * 60)

    # Ensure Athena DB exists
    run_sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")

    # Discover what ECS wrote to S3
    full_latest, inc_versions = discover()
    print(f"Discovered → full: {len(full_latest)}, incremental: {len(inc_versions)}")

    errors = []

    # ── Full tables ───────────────────────────────────────────────────────────
    for (table, version), load_ts in sorted(full_latest.items()):
        try:
            upsert_full(table, version, load_ts)
        except Exception as e:
            msg = f"full {table}/v{version}: {e}"
            print(f"  ❌ {msg}")
            errors.append(msg)

    # ── Incremental tables ────────────────────────────────────────────────────
    for table, version in sorted(inc_versions):
        try:
            upsert_inc(table, version)
        except Exception as e:
            msg = f"inc {table}/v{version}: {e}"
            print(f"  ❌ {msg}")
            errors.append(msg)

    print("=" * 60)
    if errors:
        print(f"⚠️  PARTIAL — {len(errors)} error(s):")
        for e in errors:
            print(f"    {e}")
        return {
            "status":  "partial",
            "full":    len(full_latest),
            "inc":     len(inc_versions),
            "errors":  errors,
        }

    print("✅ ALL DONE")
    return {
        "status": "ok",
        "full":   len(full_latest),
        "inc":    len(inc_versions),
    }