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


def run_platform(name, runner, url, cookie_env, buckets):
    cookie = os.environ.get(cookie_env, "")
    if not cookie:
        log(f"{name}: missing {cookie_env}; skipping")
        return
    try:
        log(f"{name}: scraping {url}")
        items = runner(url, cookie)
        log(f"{name}: scraped {len(items)} items")
        if not items:
            return
        # Only classify new/changed items (store.merge will preserve prior)
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
    handles, buckets = load_config()
    run_platform("instagram", instagram.run_sync, handles["instagram"], "IG_SESSION_COOKIE", buckets)
    run_platform("facebook",  facebook.run_sync,  handles["facebook"],  "FB_SESSION_COOKIE", buckets)
    try:
        aggregate.build()
        log("aggregate: index.json written")
    except Exception as e:
        log(f"aggregate: FAILED — {e}")


if __name__ == "__main__":
    main()
