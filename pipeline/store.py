import json
import os
from pathlib import Path
from scrapers.common import today_str

DATA_ROOT = Path("data")


def _path_for(platform: str, date_str: str) -> Path:
    return DATA_ROOT / platform / f"{date_str}.json"


def load_day(platform: str, date_str: str):
    p = _path_for(platform, date_str)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return []


def save_day(platform: str, date_str: str, items):
    p = _path_for(platform, date_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def merge(platform: str, new_items):
    """Dedup by id, keep existing classification unless text changed."""
    date_str = today_str()
    existing = load_day(platform, date_str)
    by_id = {it["id"]: it for it in existing}
    for it in new_items:
        prev = by_id.get(it["id"])
        if prev and prev.get("text") == it["text"]:
            # Preserve prior classification
            for k in ("language", "sentiment", "topic_predefined", "topic_auto"):
                if k in prev:
                    it[k] = prev[k]
        by_id[it["id"]] = it
    merged = list(by_id.values())
    save_day(platform, date_str, merged)
    return merged
