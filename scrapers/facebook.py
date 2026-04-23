import os
from apify_client import ApifyClient
from .common import now_iso

FB_COMMENT_ACTOR = "apify/facebook-comments-scraper"
FB_POST_ACTOR = "apify/facebook-posts-scraper"

TIMEOUT_SECS = 600
MAX_COMMENTS = 500
LOOKBACK_DAYS = 1


def _client():
    return ApifyClient(os.environ["APIFY_TOKEN"])


def _recent_post_urls(client, profile_url, limit=10):
    run_input = {
        "startUrls": [{"url": profile_url}],
        "resultsLimit": limit,
        "onlyPostsNewerThan": "24 hours",
    }
    run = client.actor(FB_POST_ACTOR).call(
        run_input=run_input, timeout_secs=TIMEOUT_SECS
    )
    if not run or not run.get("defaultDatasetId"):
        return []
    urls = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        url = item.get("url") or item.get("postUrl") or item.get("topLevelUrl")
        if url:
            urls.append(url.split("?")[0])
    return urls[:limit]


def _scrape_comments_for_posts(client, post_urls, handle):
    if not post_urls:
        return []
    run_input = {
        "startUrls": [{"url": u} for u in post_urls],
        "resultsLimit": MAX_COMMENTS,
        "maxCommentsPerPost": 200,
        "includeNestedComments": True,
    }
    run = client.actor(FB_COMMENT_ACTOR).call(
        run_input=run_input, timeout_secs=TIMEOUT_SECS
    )
    if not run or not run.get("defaultDatasetId"):
        return []

    out = []
    for it in client.dataset(run["defaultDatasetId"]).iterate_items():
        native_id = str(it.get("id") or it.get("commentId") or "")
        text = (it.get("text") or it.get("commentText") or "").strip()
        if not text or not native_id:
            continue
        out.append({
            "id": f"fb_{native_id}",
            "platform": "facebook",
            "handle": handle,
            "post_url": it.get("postUrl") or it.get("facebookUrl") or "",
            "parent_comment_id": it.get("parentId") or it.get("replyToId") or None,
            "author": it.get("profileName") or it.get("author") or "unknown",
            "text": text,
            "language": "unknown",
            "like_count": int(it.get("likesCount") or 0),
            "reply_count": int(it.get("repliesCount") or 0),
            "captured_at": now_iso(),
            "posted_at": it.get("date") or it.get("createdAt") or now_iso(),
        })
        if len(out) >= MAX_COMMENTS:
            break
    return out


def run_sync(profile_url: str, _unused_cookie=None):
    client = _client()
    handle = "@" + profile_url.rstrip("/").split("/")[-1]
    urls = _recent_post_urls(client, profile_url)
    print(f"[fb] recent posts last 24h: {len(urls)}")
    if not urls:
        return []
    return _scrape_comments_for_posts(client, urls, handle)
