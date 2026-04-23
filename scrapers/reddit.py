import os
import re
from datetime import datetime, timezone, timedelta
from apify_client import ApifyClient
from .common import now_iso

REDDIT_ACTOR = "trudax/reddit-scraper-lite"
TIMEOUT_SECS = 600
MAX_ITEMS = 100              # Section 12 hard daily cap
LOOKBACK_DAYS = 1           # initial wider window; tighten to 1 later


def _client():
    return ApifyClient(os.environ["APIFY_TOKEN"])

def _stable_id(item):
    """Produce a reddit_tX_id style stable ID per spec §2.6."""
    kind = None
    native = item.get("id") or ""
    # Prefer explicit type if present
    t = (item.get("dataType") or item.get("type") or "").lower()
    if "comment" in t:
        kind = "t1"
    elif "post" in t or "submission" in t:
        kind = "t3"
    else:
        # Fall back to URL heuristic
        url = item.get("url") or item.get("permalink") or ""
        kind = "t1" if "/comments/" in url and url.rstrip("/").count("/") >= 7 else "t3"
    # Strip any existing prefix from id
    native = re.sub(r"^t[1-6]_", "", str(native))
    return f"reddit_{kind}_{native}" if native else f"reddit_{kind}_{abs(hash(item.get('url','')))}"


def _to_record(item, matched_term):
    data_type = (item.get("dataType") or "").lower()
    kind_is_comment = "comment" in data_type

    title = (item.get("title") or "").strip()
    body = (item.get("body") or "").strip()
    # Strip the "Thumbnail: https://..." boilerplate the lite actor
    # sometimes puts in body for gallery posts
    if body.startswith("Thumbnail:"):
        body = ""
    if kind_is_comment:
        full_text = body
    else:
        full_text = f"{title}\n\n{body}".strip() if body else title
    if not full_text:
        return None

    url = item.get("url") or ""
    if url.startswith("/r/"):
        url = "https://www.reddit.com" + url

    # parsedCommunityName is "BollywoodFashion" (no r/ prefix)
    subreddit = item.get("parsedCommunityName") or (
        (item.get("communityName") or "").lstrip("r/")
    )

    # The actor sets id like "t3_1sk7wno" for posts, "t1_xxx" for comments
    stable_id = item.get("id") or ""
    if stable_id and not stable_id.startswith("reddit_"):
        stable_id = f"reddit_{stable_id}"
    if not stable_id:
        stable_id = f"reddit_t3_{item.get('parsedId') or abs(hash(url))}"

    return {
        "id": stable_id,
        "platform": "reddit",
        "handle": None,
        "post_url": url,
        "parent_comment_id": None,          # lite actor doesn't expose comment parent IDs
        "author": item.get("username") or "[deleted]",
        "text": full_text,
        "language": "unknown",
        "like_count": int(item.get("upVotes") or 0),
        "reply_count": int(item.get("numberOfComments") or 0),
        "captured_at": now_iso(),
        "posted_at": item.get("createdAt") or now_iso(),
        "reddit_subreddit": subreddit,
        "reddit_item_type": "comment" if kind_is_comment else "post",
        "reddit_matched_term": matched_term,
        "reddit_filter_result": None,
    }

def _is_recent(record, cutoff_iso):
    try:
        dt = datetime.fromisoformat(record["posted_at"].replace("Z", "+00:00"))
    except Exception:
        return True
    return dt >= datetime.fromisoformat(cutoff_iso)


def _match_strict(text, terms):
    low = text.lower()
    for t in terms:
        if t.lower() in low:
            return t
    return None


def run_sync(brand_terms):
    """brand_terms: dict with keys 'strict' and 'ambiguous'."""
    strict = brand_terms.get("strict", []) or []
    ambiguous = brand_terms.get("ambiguous", []) or []
    all_terms = strict + ambiguous
    if not all_terms:
        return []

    client = _client()
    run_input = {
        "searches": all_terms,
        "type": "posts",
        "sort": "new",
        "maxItems": MAX_ITEMS,
        "maxPostCount": MAX_ITEMS,
        "maxComments": 30,
        "maxCommunitiesCount": 0,
        "maxUserCount": 0,
        "scrollTimeout": 60,
        "proxy": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
            "apifyProxyCountry": "US",
        },
    }
    run = client.actor(REDDIT_ACTOR).call(
        run_input=run_input, timeout_secs=TIMEOUT_SECS
    )
    if not run or not run.get("defaultDatasetId"):
        return []

    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    seen = set()
    out = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        blob = " ".join([
            str(item.get("title") or ""),
            str(item.get("body") or ""),
            str(item.get("url") or ""),
            str(item.get("communityName") or ""),
        ])
        matched = _match_strict(blob, all_terms)
        if not matched:
            continue

        rec = _to_record(item, matched)
        if not rec:
            continue
        if rec["id"] in seen:
            continue
        if not _is_recent(rec, cutoff):
            continue

        rec["_term_class"] = "strict" if matched in strict else "ambiguous"
        seen.add(rec["id"])
        out.append(rec)
        if len(out) >= MAX_ITEMS:
            break
    return out
