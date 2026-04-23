import os
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from google import genai
from google.genai import types

MODEL = "gemini-2.5-flash"

RESPONSE_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["relevant", "not_relevant", "uncertain"]},
            "reason":  {"type": "string"},
        },
        "required": ["verdict", "reason"],
    },
}


def _client():
    return genai.Client(api_key=os.environ["GEMINI_API_KEY"])


def _build_prompt(items):
    lines = []
    for i, it in enumerate(items):
        lines.append(
            f"{i}. term={json.dumps(it['reddit_matched_term'])} "
            f"subreddit=r/{it.get('reddit_subreddit','')} "
            f"text={json.dumps(it['text'][:1200])}"
        )
    return (
        "You are filtering Reddit matches for the Indian luxury fashion brand 'Aza Fashions'. "
        "For each item, decide whether the mention is actually about this brand.\n\n"
        "Rules:\n"
        "- 'relevant': clearly about the fashion brand Aza Fashions\n"
        "- 'not_relevant': about a different Aza (person, place, unrelated company, acronym)\n"
        "- 'uncertain': ambiguous from context\n\n"
        "Return a JSON array, one object per item, in order:\n\n"
        + "\n".join(lines)
    )


def filter_ambiguous(items, log_dir="logs"):
    """Mutates items in place. For items with _term_class == 'ambiguous',
    sets reddit_filter_result to 'relevant' | 'uncertain' and DROPS
    'not_relevant' items (they're logged to logs/reddit-rejected-*.jsonl).
    Strict items pass through unchanged."""
    ambiguous = [it for it in items if it.get("_term_class") == "ambiguous"]
    strict = [it for it in items if it.get("_term_class") != "ambiguous"]
    for it in strict:
        it["reddit_filter_result"] = None
        it.pop("_term_class", None)

    if not ambiguous:
        return strict

    client = _client()
    try:
        resp = client.models.generate_content(
            model=MODEL,
            contents=_build_prompt(ambiguous),
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=RESPONSE_SCHEMA,
                temperature=0.1,
                max_output_tokens=2048,
            ),
        )
        parsed = json.loads((resp.text or "").strip())
    except Exception as e:
        print(f"[reddit-filter] failed: {e}; keeping all ambiguous items as uncertain")
        parsed = [{"verdict": "uncertain", "reason": "filter call failed"} for _ in ambiguous]

    kept = []
    rejected = []
    for it, p in zip(ambiguous, (parsed + [{}] * len(ambiguous))[:len(ambiguous)]):
        verdict = (p.get("verdict") or "uncertain").lower()
        reason = p.get("reason") or ""
        it.pop("_term_class", None)
        if verdict == "not_relevant":
            rejected.append({
                "id": it["id"],
                "text": it["text"][:500],
                "subreddit": it.get("reddit_subreddit"),
                "matched_term": it.get("reddit_matched_term"),
                "reason": reason,
                "rejected_at": datetime.now(timezone.utc).isoformat(),
            })
        else:
            it["reddit_filter_result"] = verdict  # 'relevant' or 'uncertain'
            kept.append(it)

    if rejected:
        Path(log_dir).mkdir(exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = Path(log_dir) / f"reddit-rejected-{date_str}.jsonl"
        with path.open("a", encoding="utf-8") as f:
            for r in rejected:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return strict + kept
