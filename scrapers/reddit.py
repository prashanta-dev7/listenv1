import os
import re
from datetime import datetime, timezone, timedelta
from apify_client import ApifyClient
from .common import now_iso

REDDIT_ACTOR = "trudax/reddit-scraper"
TIMEOUT_SECS = 600
MAX_ITEMS = 200              # Section 12 hard daily cap
LOOKBACK_DAYS = 7            # initial wider window; tighten to 1 later


def _client():
    return ApifyClient(os.environ["APIFY_TOKEN"])


def _build_searches(terms):
    """trudax/reddit-scraper accepts searchQueries for Reddit-wide search."""
    return [{"query": t, "sort": "new"} for t in terms]


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
    text = (item.get("body") or item.get("text") or item.get("selftext") or "").strip()
    title = (item.get("title") or "").strip()
    # For posts, combine title + body for classification context
    kind_is_comment = "comment" in (item.get("dataType") or item.get("type") or "").lower()
    full_text = text if kind_is_comment else (f"{title}\n\n{text}".strip() if text else title)
    if not full_text:
        return None

    url = item.get("url") or item.get("permalink") or ""
    if url.startswith("/r/"):
        url = "https://www.reddit.com" + url

    return {
        "id": _stable_id(item),
        "platform": "reddit",
        "handle": None,
        "post_url": url,
        "parent_comment_id": (
            f"reddit_t3_{item['postId']}" if item.get("postId") and kind_is_comment else None
        ),
        "author": item.get("username") or item.get("author") or "[deleted]",
        "text": full_text,
        "language": "unknown",
        "like_count": int(item.get("score") or item.get("upVotes") or 0),
        "reply_count": int(item.get("numberOfComments") or item.get("numReplies") or 0),
        "captured_at": now_iso(),
        "posted_at": (
            item.get("createdAt")
            or item.get("created")
            or item.get("date")
            or now_iso()
        ),
        "reddit_subreddit": (item.get("communityName") or item.get("subreddit") or "").lstrip("r/"),
        "reddit_item_type": "comment" if kind_is_comment else "post",
        "reddit_matched_term": matched_term,
        "reddit_filter_result": None,  # set later by filter pass if ambiguous
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
    """brand_terms: dict with keys 'strict' and 'ambiguous' (lists of strings).
    Returns list of records conforming to Section 5.2 schema."""
    strict = brand_terms.get("strict", []) or []
    ambiguous = brand_terms.get("ambiguous", []) or []
    all_terms = strict + ambiguous
    if not all_terms:
        return []

    client = _client()
    run_input = {
        "searches": _build_searches(all_terms),
        "maxItems": MAX_ITEMS,
        "maxComments": 30,
        "maxCommunitiesCount": 0,
        "scrollTimeout": 40,
        "proxy": {"useApifyProxy": True},
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
        # Identify which term actually matched (actor doesn't echo it back)
        blob = " ".join([
            str(item.get("title") or ""),
            str(item.get("body") or ""),
            str(item.get("text") or ""),
            str(item.get("selftext") or ""),
            str(item.get("url") or ""),
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

        # Tag which class of term triggered the match
        rec["_term_class"] = "strict" if matched in strict else "ambiguous"
        seen.add(rec["id"])
        out.append(rec)
        if len(out) >= MAX_ITEMS:
            break
    return out
