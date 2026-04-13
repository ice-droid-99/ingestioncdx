from datetime import datetime, timezone

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def utc_today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")