import json
from pathlib import Path
from collections import Counter, defaultdict

DATA_ROOT = Path("data")
INDEX_PATH = DATA_ROOT / "index.json"
PLATFORMS_ALL = ["instagram", "facebook", "reddit"]

def build():
    from collections import Counter, defaultdict
    PLATFORMS = [p for p in PLATFORMS_ALL
                 if (DATA_ROOT / p).exists() and any((DATA_ROOT / p).glob("*.json"))] \
                or ["instagram"]

    by_day = defaultdict(lambda: {p: 0 for p in PLATFORMS})
    sentiment_by_platform = {p: Counter() for p in PLATFORMS}
    predefined_counts = Counter()
    auto_theme_counts = Counter()
    auto_theme_examples = {}
    top_commenters = Counter()
    days_by_platform = {p: [] for p in PLATFORMS}

    # Reddit-specific aggregates (Section 6.3)
    subreddit_counts = Counter()
    subreddit_sentiment = defaultdict(Counter)
    uncertain_count = 0

    for p in PLATFORMS:
        pdir = DATA_ROOT / p
        if not pdir.exists():
            continue
        for fp in sorted(pdir.glob("*.json")):
            day = fp.stem
            items = json.loads(fp.read_text(encoding="utf-8"))
            days_by_platform[p].append(day)
            by_day[day][p] += len(items)
            for it in items:
                if it.get("language") == "english" and it.get("sentiment"):
                    sentiment_by_platform[p][it["sentiment"]] += 1
                if it.get("topic_predefined"):
                    predefined_counts[it["topic_predefined"]] += 1
                if it.get("topic_auto"):
                    auto_theme_counts[it["topic_auto"]] += 1
                    auto_theme_examples.setdefault(it["topic_auto"], it.get("text","")[:200])
                top_commenters[(p, it.get("author") or "unknown")] += 1

                if p == "reddit":
                    sub = it.get("reddit_subreddit") or "unknown"
                    subreddit_counts[sub] += 1
                    if it.get("sentiment"):
                        subreddit_sentiment[sub][it["sentiment"]] += 1
                    if it.get("reddit_filter_result") == "uncertain":
                        uncertain_count += 1

    subreddit_panel = []
    for sub, count in subreddit_counts.most_common(10):
        dominant = subreddit_sentiment[sub].most_common(1)
        subreddit_panel.append({
            "subreddit": sub,
            "count": count,
            "dominant_sentiment": dominant[0][0] if dominant else None,
        })

    summary = {
        "generated_at": __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "platforms": PLATFORMS,
        "days_by_platform": days_by_platform,
        "volume_by_day": [{"date": d, **counts} for d, counts in sorted(by_day.items())],
        "sentiment_by_platform": {p: dict(sentiment_by_platform[p]) for p in PLATFORMS},
        "predefined_topics": dict(predefined_counts),
        "auto_themes": [
            {"theme": t, "count": c, "example": auto_theme_examples.get(t, "")}
            for t, c in auto_theme_counts.most_common(50)
        ],
        "top_commenters": [
            {"author": a, "platform": p, "count": c}
            for (p, a), c in top_commenters.most_common(50)
        ],
        "reddit_subreddits": subreddit_panel,
        "reddit_uncertain_count": uncertain_count,
    }
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary
