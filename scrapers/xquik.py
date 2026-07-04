import csv
import hashlib
import json
from pathlib import Path

from .common import now_iso

TEXT_FIELDS = ("text", "tweet_text", "full_text", "content", "body")
DATE_FIELDS = ("created_at", "posted_at", "timestamp", "date")
LIKE_FIELDS = ("like_count", "likes", "favorite_count")
REPLY_FIELDS = ("reply_count", "replies", "comment_count", "comments")


def _rows_from_json(value):
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("data", "tweets", "results", "items"):
            rows = value.get(key)
            if isinstance(rows, list):
                return rows
    return []


def _load_rows(path):
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as handle:
            return list(csv.DictReader(handle))
    if suffix == ".jsonl":
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
        return rows
    return _rows_from_json(json.loads(path.read_text(encoding="utf-8")))


def _first(row, fields, default=""):
    for field in fields:
        value = row.get(field)
        if value not in (None, ""):
            return value
    return default


def _count(row, fields):
    raw = _first(row, fields, 0)
    try:
        return int(float(str(raw).replace(",", "")))
    except (TypeError, ValueError):
        return 0


def _stable_id(row, text, posted_at):
    native_id = row.get("id") or row.get("tweet_id") or row.get("post_id")
    if native_id:
        return f"xquik_{native_id}"
    digest = hashlib.sha1(f"{posted_at}:{text}".encode("utf-8")).hexdigest()[:16]
    return f"xquik_{digest}"


def _post_url(row):
    url = row.get("url") or row.get("tweet_url") or row.get("post_url")
    if url:
        return url
    native_id = row.get("id") or row.get("tweet_id")
    return f"https://x.com/i/web/status/{native_id}" if native_id else ""


def _normalize(row):
    text = str(_first(row, TEXT_FIELDS)).strip()
    if not text:
        return None
    posted_at = str(_first(row, DATE_FIELDS, now_iso()))
    handle = row.get("username") or row.get("author") or row.get("handle") or "xquik"
    return {
        "id": _stable_id(row, text, posted_at),
        "platform": "xquik",
        "handle": str(handle).lstrip("@"),
        "post_url": _post_url(row),
        "parent_comment_id": row.get("conversation_id") or None,
        "author": str(handle).lstrip("@"),
        "text": text,
        "language": "unknown",
        "like_count": _count(row, LIKE_FIELDS),
        "reply_count": _count(row, REPLY_FIELDS),
        "retweet_count": _count(row, ("retweet_count", "retweets", "reposts")),
        "captured_at": now_iso(),
        "posted_at": posted_at,
    }


def run_sync(export_path):
    path = Path(export_path)
    if not path.exists():
        print(f"[xquik] export not found: {path}")
        return []
    items = []
    for row in _load_rows(path):
        if isinstance(row, dict):
            item = _normalize(row)
            if item:
                items.append(item)
    print(f"[xquik] imported {len(items)} export rows")
    return items
