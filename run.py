import os
import json
import traceback
from pathlib import Path
from datetime import datetime, timezone

from scrapers import instagram, facebook
from pipeline import classify, store, aggregate

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
    topics = json.loads(Path("config/topics.json").read_text())
    return handles, topics["buckets"]


def run_platform(name, runner, url, buckets):
    try:
        log(f"{name}: scraping {url} via Apify")
        items = runner(url)
        log(f"{name}: scraped {len(items)} items")
        if not items:
            return
        merged_preview = store.load_day(name, datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        prev_by_id = {it["id"]: it for it in merged_preview}
        needs_class = [
            it for it in items
            if prev_by_id.get(it["id"], {}).get("text") != it["text"]
        ]
        log(f"{name}: classifying {len(needs_class)} new/changed items")
        classify.classify(needs_class, buckets)
        store.merge(name, items)
    except Exception as e:
        log(f"{name}: FAILED — {e}\n{traceback.format_exc()}")


def main():
    if not os.environ.get("APIFY_TOKEN"):
        log("APIFY_TOKEN missing; aborting run")
        return
    if not os.environ.get("GEMINI_API_KEY"):
        log("GEMINI_API_KEY missing; aborting run")
        return

    handles, buckets = load_config()
    run_platform("instagram", instagram.run_sync, handles["instagram"], buckets)
    run_platform("facebook",  facebook.run_sync,  handles["facebook"],  buckets)
    try:
        aggregate.build()
        log("aggregate: index.json written")
    except Exception as e:
        log(f"aggregate: FAILED — {e}")


if __name__ == "__main__":
    main()
