import os
from apify_client import ApifyClient
from .common import now_iso, within_last_24h

IG_COMMENT_ACTOR = "apify/instagram-comment-scraper"
IG_POST_ACTOR    = "apify/instagram-post-scraper"

TIMEOUT_SECS = 600
MAX_COMMENTS = 500


def _client():
    return ApifyClient(os.environ["APIFY_TOKEN"])


from datetime import datetime, timezone, timedelta

LOOKBACK_DAYS = 7  # widen from 24h until steady-state dataset accumulates

def _recent_post_urls(client, profile_url, limit=20):
    username = profile_url.rstrip("/").split("/")[-1]
    run_input = {
        "username": [username],
        "resultsLimit": limit,
        "resultsType": "posts",
    }
    run = client.actor(IG_POST_ACTOR).call(
        run_input=run_input, timeout_secs=TIMEOUT_SECS
    )
    if not run or not run.get("defaultDatasetId"):
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    urls = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        ts = item.get("timestamp") or item.get("taken_at") or ""
        url = item.get("url") or item.get("postUrl")
        try:
            posted = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            posted = None
        if url and (posted is None or posted >= cutoff):
            urls.append(url)
    return urls[:limit]

def _scrape_comments_for_posts(client, post_urls, handle):
    if not post_urls:
        return []
    run_input = {
        "directUrls": post_urls,
        "resultsLimit": MAX_COMMENTS,
         "resultsPerPage": 200, 
        "isNewestComments": True,
        "includeReplies": True,
    }
    run = client.actor(IG_COMMENT_ACTOR).call(
        run_input=run_input, timeout_secs=TIMEOUT_SECS
    )
    if not run or not run.get("defaultDatasetId"):
        return []

    out = []
    for it in client.dataset(run["defaultDatasetId"]).iterate_items():
        native_id = str(it.get("id") or it.get("commentId") or "")
        text = (it.get("text") or "").strip()
        if not text or not native_id:
            continue
        out.append({
            "id": f"ig_{native_id}",
            "platform": "instagram",
            "handle": handle,
            "post_url": it.get("postUrl") or it.get("url") or "",
            "parent_comment_id": it.get("replyToId") or it.get("parentId") or None,
            "author": (it.get("ownerUsername") or it.get("username") or "").lstrip("@"),
            "text": text,
            "language": "unknown",
            "like_count": int(it.get("likesCount") or it.get("likeCount") or 0),
            "reply_count": int(it.get("repliesCount") or 0),
            "captured_at": now_iso(),
            "posted_at": it.get("timestamp") or it.get("createdAt") or now_iso(),
        })
        if len(out) >= MAX_COMMENTS:
            break
    return out


def run_sync(profile_url: str, _unused_cookie=None):
    client = _client()
    handle = "@" + profile_url.rstrip("/").split("/")[-1]
    urls = _recent_post_urls(client, profile_url)
    print(f"[ig] recent posts last 24h: {len(urls)}")
    if not urls:
        return []
    return _scrape_comments_for_posts(client, urls, handle)
