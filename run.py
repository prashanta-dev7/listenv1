import os, json, traceback
from pathlib import Path
from datetime import datetime, timezone

from scrapers import instagram, facebook, reddit, tiktok
from pipeline import classify, filter as reddit_filter, store, aggregate

LOGS = Path("logs")
LOGS.mkdir(exist_ok=True)
LOG_PATH = LOGS / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.log"


def log(msg):
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}\n"
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line)
    print(line.rstrip())


def load_config():
    handles = json.loads(Path("config/handles.json").read_text())
    topics  = json.loads(Path("config/topics.json").read_text())
    terms_path = Path("config/brand-terms.json")
    terms = json.loads(terms_path.read_text()) if terms_path.exists() else {"strict": [], "ambiguous": []}
    return handles, topics["buckets"], terms


def run_platform(name, runner, url_or_terms, buckets, is_reddit=False):
    try:
        if is_reddit:
            log(f"{name}: scraping with terms={url_or_terms.get('strict', [])} via Apify")
            items = runner(url_or_terms)
            log(f"{name}: scraped {len(items)} raw items")
            items = reddit_filter.filter_ambiguous(items, log_dir="logs")
            log(f"{name}: {len(items)} items after filter")
        else:
            log(f"{name}: scraping {url_or_terms} via Apify")
            items = runner(url_or_terms)
            log(f"{name}: scraped {len(items)} items")

        if not items:
            return

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        prev_by_id = {it["id"]: it for it in store.load_day(name, today)}
        needs_class = [it for it in items if prev_by_id.get(it["id"], {}).get("text") != it["text"]]
        log(f"{name}: classifying {len(needs_class)} new/changed items")
        classify.classify(needs_class, buckets)
        store.merge(name, items)
    except Exception as e:
        log(f"{name}: FAILED — {e}\n{traceback.format_exc()}")

from scrapers import tiktok   # plus existing imports

# ... existing IG, FB, Reddit calls ...

try:
    log("tiktok: scraping via Apify")
    items = tiktok.run_sync(handles["tiktok"])
    log(f"tiktok: scraped {len(items)} items")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prev_by_id = {it["id"]: it for it in store.load_day("tiktok", today)}
    needs_class = [it for it in items if prev_by_id.get(it["id"], {}).get("text") != it["text"]]
    classify.classify(needs_class, buckets)
    store.merge("tiktok", items)
except Exception as e:
    log(f"tiktok: FAILED — {e}")

# Twitter + Quora wiring parked for v2.1 — leave commented out for easy re-enable:
# try:
#     log("twitter: scraping via Apify")
#     items = twitter.run_sync(handles["twitter"], terms)
#     ...
# except Exception as e:
#     log(f"twitter: FAILED — {e}")

def main():
    if not os.environ.get("APIFY_TOKEN"):
        log("APIFY_TOKEN missing; aborting run"); return
    if not os.environ.get("GEMINI_API_KEY"):
        log("GEMINI_API_KEY missing; aborting run"); return

    handles, buckets, terms = load_config()
    run_platform("instagram", instagram.run_sync, handles["instagram"], buckets)
    run_platform("facebook",  facebook.run_sync,  handles["facebook"],  buckets)
    run_platform("reddit",    reddit.run_sync,    terms,                buckets, is_reddit=True)

    try:
        aggregate.build()
        log("aggregate: index.json written")
    except Exception as e:
        log(f"aggregate: FAILED — {e}")


if __name__ == "__main__":
    main()
