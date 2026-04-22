import json
import re
from datetime import datetime, timezone

UTC = timezone.utc


def now_iso():
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_str():
    return datetime.now(UTC).strftime("%Y-%m-%d")


def parse_cookie_blob(blob: str, domain: str):
    """Accept either a JSON array (Cookie-Editor JSON export) or a
    'name=value; name2=value2' header string. Return list of Playwright
    cookie dicts scoped to `domain`."""
    if not blob:
        return []
    blob = blob.strip()
    if blob.startswith("["):
        raw = json.loads(blob)
        out = []
        for c in raw:
            out.append({
                "name": c["name"],
                "value": c["value"],
                "domain": c.get("domain", domain),
                "path": c.get("path", "/"),
                "httpOnly": c.get("httpOnly", False),
                "secure": c.get("secure", True),
                "sameSite": (c.get("sameSite") or "Lax").capitalize()
                            if (c.get("sameSite") or "").lower() in ("lax", "strict", "none")
                            else "Lax",
            })
        return out

    # Header string: "k=v; k2=v2"
    out = []
    for part in re.split(r";\s*", blob):
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        out.append({
            "name": name.strip(),
            "value": value.strip(),
            "domain": domain,
            "path": "/",
            "httpOnly": False,
            "secure": True,
            "sameSite": "Lax",
        })
    return out


def within_last_24h(posted_at_iso: str) -> bool:
    try:
        dt = datetime.fromisoformat(posted_at_iso.replace("Z", "+00:00"))
    except Exception:
        return True  # if unknown, keep it; filtered later if needed
    delta = datetime.now(UTC) - dt
    return 0 <= delta.total_seconds() <= 24 * 3600
