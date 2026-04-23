import os
from datetime import datetime, timezone, timedelta
from apify_client import ApifyClient
from .common import now_iso

TIKTOK_PROFILE_ACTOR = "clockworks/tiktok-scraper"
TIKTOK_COMMENT_ACTOR = "clockworks/tiktok-comments-scraper"
TIMEOUT_SECS = 900
MAX_VIDEOS = 20
MAX_COMMENTS = 500
LOOKBACK_DAYS = 30

def _client():
    return ApifyClient(os.environ["APIFY_TOKEN"])

def _stable_id(prefix, native):
    return f"tiktok_{prefix}_{native}"

def _to_comment_record(c, video_url, handle):
    text = (c.get("text") or "").strip()
    if not text: return None
    cid = c.get("cid") or c.get("id") or ""
    return {
        "id": _stable_id("c", cid),
        "platform": "tiktok",
        "handle": handle,
        "post_url": video_url,
        "parent_comment_id": _stable_id("v", c.get("videoWebUrl") or c.get("awemeId") or ""),
        "author": (c.get("user") or {}).get("uniqueId") or c.get("uniqueId") or "unknown",
        "text": text,
        "language": "unknown",
        "like_count": int(c.get("diggCount") or 0),
        "reply_count": int(c.get("replyCommentTotal") or 0),
        "captured_at": now_iso(),
        "posted_at": c.get("createTimeISO") or now_iso(),
        "tiktok_item_type": "comment",
    }

def _recent_video_urls(client, handle, limit=MAX_VIDEOS):
    run_input = { ... }   # unchanged
    run = client.actor(TIKTOK_PROFILE_ACTOR).call(run_input=run_input, timeout_secs=TIMEOUT_SECS)
    if not run or not run.get("defaultDatasetId"):
        print("DEBUG tiktok: profile actor returned no dataset")
        return []
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"DEBUG tiktok: profile scraper returned {len(items)} raw videos")
    if items:
    print(f"DEBUG tiktok: first video createTime={items[0].get('createTimeISO') or items[0].get('createTime')}")
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    urls = []
    for v in client.dataset(run["defaultDatasetId"]).iterate_items():
        url = v.get("webVideoUrl") or v.get("url")
        ts = v.get("createTimeISO") or v.get("createTime")
        try:
            posted = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            posted = None
        if url and (posted is None or posted >= cutoff):
            urls.append(url)
    return urls[:limit]

def run_sync(handle):
    client = _client()
    video_urls = _recent_video_urls(client, handle)
    if not video_urls: return []
    run_input = {
        "postURLs": video_urls,
        "commentsPerPost": 200,
        "maxRepliesPerComment": 10,
    }
    run = client.actor(TIKTOK_COMMENT_ACTOR).call(run_input=run_input, timeout_secs=TIMEOUT_SECS)
    if not run or not run.get("defaultDatasetId"): return []

    seen, out = set(), []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        vurl = item.get("videoWebUrl") or (video_urls[0] if video_urls else "")
        rec = _to_comment_record(item, vurl, handle)
        if not rec: continue
        if rec["id"] in seen: continue
        seen.add(rec["id"])
        out.append(rec)
        if len(out) >= MAX_COMMENTS: break
    return out
