import os
from datetime import datetime, timezone, timedelta
from apify_client import ApifyClient
from .common import now_iso

QUORA_ACTOR = "epctex/quora-scraper"
TIMEOUT_SECS = 600
MAX_ITEMS = 100
LOOKBACK_DAYS = 30   # Quora threads update slowly; wider window

def _client():
    return ApifyClient(os.environ["APIFY_TOKEN"])

def _stable_id(item):
    qid = item.get("id") or item.get("url") or ""
    return f"quora_{abs(hash(qid))}"

def _to_record(item, matched_term):
    text = (item.get("answer") or item.get("text") or item.get("content") or "").strip()
    title = (item.get("question") or item.get("title") or "").strip()
    full = f"{title}\n\n{text}".strip() if text else title
    if not full:
        return None
    url = item.get("url") or item.get("permalink") or ""
    return {
        "id": _stable_id(item),
        "platform": "quora",
        "handle": None,
        "post_url": url,
        "parent_comment_id": None,
        "author": item.get("author") or item.get("username") or "anonymous",
        "text": full,
        "language": "unknown",
        "like_count": int(item.get("upvotes") or item.get("likes") or 0),
        "reply_count": int(item.get("answersCount") or item.get("commentsCount") or 0),
        "captured_at": now_iso(),
        "posted_at": item.get("createdAt") or item.get("date") or now_iso(),
        "quora_matched_term": matched_term,
        "quora_item_type": "answer" if text else "question",
    }

def run_sync(brand_terms):
    terms = brand_terms.get("strict", []) + brand_terms.get("platform_extras", {}).get("quora", [])
    if not terms: return []
    client = _client()
    run_input = {
        "search": terms,
        "maxItems": MAX_ITEMS,
        "includeAnswers": True,
        "includeComments": False,
        "proxy": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]},
    }
    run = client.actor(QUORA_ACTOR).call(run_input=run_input, timeout_secs=TIMEOUT_SECS)
    if not run or not run.get("defaultDatasetId"):
        return []

    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    seen, out = set(), []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        blob = " ".join([str(item.get(k) or "") for k in ("question","title","answer","text")]).lower()
        matched = next((t for t in terms if t.lower() in blob), None)
        if not matched: continue
        rec = _to_record(item, matched)
        if not rec: continue
        if rec["id"] in seen: continue
        seen.add(rec["id"])
        out.append(rec)
        if len(out) >= MAX_ITEMS: break
    return out
