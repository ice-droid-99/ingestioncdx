from datetime import datetime, timezone

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def utc_today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def utc_compact_ts():
    # example: 20260414T064209Z
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")