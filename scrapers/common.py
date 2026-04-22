from datetime import datetime, timezone

UTC = timezone.utc

def now_iso():
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

def today_str():
    return datetime.now(UTC).strftime("%Y-%m-%d")

def within_last_24h(posted_at_iso: str) -> bool:
    if not posted_at_iso:
        return True
    try:
        dt = datetime.fromisoformat(posted_at_iso.replace("Z", "+00:00"))
    except Exception:
        return True
    return 0 <= (datetime.now(UTC) - dt).total_seconds() <= 24 * 3600
