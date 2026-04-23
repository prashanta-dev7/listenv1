import os, re
from datetime import datetime, timezone, timedelta
from apify_client import ApifyClient
from .common import now_iso

TWITTER_ACTOR = "apidojo/tweet-scraper"
TIMEOUT_SECS = 600
MAX_ITEMS = 200
LOOKBACK_DAYS = 7

def _client():
    return ApifyClient(os.environ["APIFY_TOKEN"])

def _stable_id(tweet_id):
    tweet_id = re.sub(r"^twitter_", "", str(tweet_id or ""))
    return f"twitter_{tweet_id}"

def _to_record(t, matched_term=None):
    text = (t.get("text") or t.get("full_text") or "").strip()
    if not text:
        return None
    tid = t.get("id") or t.get("id_str") or ""
    author = (t.get("author") or {}).get("userName") or t.get("user", {}).get("screen_name") or "unknown"
    url = t.get("url") or f"https://x.com/{author}/status/{tid}"
    return {
        "id": _stable_id(tid),
        "platform": "twitter",
        "handle": "Aza_Fashions",
        "post_url": url,
        "parent_comment_id": _stable_id(t.get("conversationId")) if t.get("inReplyToId") else None,
        "author": author,
        "text": text,
        "language": (t.get("lang") or "unknown"),
        "like_count": int(t.get("likeCount") or t.get("favorite_count") or 0),
        "reply_count": int(t.get("replyCount") or 0),
        "captured_at": now_iso(),
        "posted_at": t.get("createdAt") or t.get("created_at") or now_iso(),
        "twitter_matched_term": matched_term,
        "twitter_is_reply": bool(t.get("inReplyToId")),
    }

def _is_recent(rec, cutoff_iso):
    try:
        dt = datetime.fromisoformat(str(rec["posted_at"]).replace("Z", "+00:00"))
    except Exception:
        return True
    return dt >= datetime.fromisoformat(cutoff_iso)

def run_sync(handle, brand_terms):
    client = _client()
    all_terms = brand_terms.get("strict", []) + brand_terms.get("platform_extras", {}).get("twitter", [])
    start_urls = [
        f"https://x.com/{handle}",
        f"https://x.com/{handle}/with_replies",
    ]
    run_input = {
        "startUrls": start_urls,
        "searchTerms": all_terms,
        "maxItems": MAX_ITEMS,
        "sort": "Latest",
        "tweetLanguage": "en",
        "includeSearchTerms": True,
        "onlyImage": False, "onlyVideo": False,
    }
    run = client.actor(TWITTER_ACTOR).call(run_input=run_input, timeout_secs=TIMEOUT_SECS)
    if not run or not run.get("defaultDatasetId"):
        return []

    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    seen, out = set(), []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        blob = ((item.get("text") or "") + " " + (item.get("url") or "")).lower()
        matched = next((t for t in all_terms if t.lower() in blob), None)
        rec = _to_record(item, matched)
        if not rec: continue
        if rec["id"] in seen: continue
        if not _is_recent(rec, cutoff): continue
        seen.add(rec["id"])
        out.append(rec)
        if len(out) >= MAX_ITEMS: break
    return out
