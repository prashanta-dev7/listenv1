import os
import json
import time
from google import genai
from google.genai import types

MODEL = "gemini-2.5-flash"
BATCH_SIZE = 25  # Gemini Flash is cheap + fast; larger batches are fine


def _client():
    return genai.Client(api_key=os.environ["GEMINI_API_KEY"])


# JSON schema returned by the model for each comment
ITEM_SCHEMA = {
    "type": "object",
    "properties": {
        "language":         {"type": "string", "enum": ["english", "non-english"]},
        "sentiment":        {"type": "string", "enum": ["positive", "negative", "neutral", "none"]},
        "topic_predefined": {"type": "string"},
        "topic_auto":       {"type": "string"},
    },
    "required": ["language", "sentiment", "topic_predefined", "topic_auto"],
}

RESPONSE_SCHEMA = {"type": "array", "items": ITEM_SCHEMA}

def _build_prompt(items, buckets):
    bucket_lines = "\n".join(f"- {b['id']}: {b['description']}" for b in buckets)
    bucket_ids = [b["id"] for b in buckets]

    def _fmt(i, it):
    p = it.get("platform")
    extra = ""
    if p == "reddit":
        extra = f" [reddit {it.get('reddit_item_type','post')} in r/{it.get('reddit_subreddit','')}]"
    elif p == "twitter":
        extra = f" [tweet{' (reply)' if it.get('twitter_is_reply') else ''}]"
    elif p == "quora":
        extra = f" [quora {it.get('quora_item_type','answer')}]"
    elif p == "tiktok":
        extra = f" [tiktok comment on @{it.get('handle','azafashions')}]"
    return f"{i}. {json.dumps(it['text'])}{extra}"

    item_lines = "\n".join(_fmt(i, it) for i, it in enumerate(items))
    return f"""You are classifying social media comments for a fashion brand (Aza Fashions).

For EACH numbered comment, return an object with:
- "language": "english" or "non-english"
- "sentiment": "positive" | "negative" | "neutral" for english comments; "none" for non-english
- "topic_predefined": exactly one of [{", ".join(bucket_ids)}] — or the string "none" if nothing fits
- "topic_auto": a short 2-4 word free-form theme label (english only); "none" for non-english

Predefined buckets:
{bucket_lines}

Return a JSON array of length {len(items)}, in the SAME order as the input.
Do not include any prose. Comments:

{item_lines}
"""


def _normalize(p):
    """Map 'none' strings to Python None and guard against missing fields."""
    out = {
        "language": p.get("language") or "unknown",
        "sentiment": p.get("sentiment"),
        "topic_predefined": p.get("topic_predefined"),
        "topic_auto": p.get("topic_auto"),
    }
    if out["language"] != "english":
        out["sentiment"] = None
    if out["sentiment"] == "none":
        out["sentiment"] = None
    if out["topic_predefined"] in ("none", "", None):
        out["topic_predefined"] = None
    if out["topic_auto"] in ("none", "", None):
        out["topic_auto"] = None
    return out


def classify(items, buckets):
    """Mutates each item in-place with language/sentiment/topic fields."""
    if not items:
        return items
    client = _client()

    for start in range(0, len(items), BATCH_SIZE):
        batch = items[start:start + BATCH_SIZE]
        prompt = _build_prompt(batch, buckets)

        parsed = None
        for attempt in range(3):
            try:
                resp = client.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=RESPONSE_SCHEMA,
                        temperature=0.2,
                        max_output_tokens=4096,
                    ),
                )
                text = (resp.text or "").strip()
                parsed = json.loads(text)
                if not isinstance(parsed, list):
                    raise ValueError("Model did not return a JSON array")
                # Pad / truncate to batch length
                parsed = (parsed + [{}] * len(batch))[:len(batch)]
                break
            except Exception as e:
                print(f"[classify] attempt {attempt+1} failed: {e}")
                time.sleep(2 ** attempt)

        if parsed is None:
            # Give up for this batch; mark unknown but keep pipeline moving
            for it in batch:
                it["language"] = "unknown"
                it["sentiment"] = None
                it["topic_predefined"] = None
                it["topic_auto"] = None
            continue

        for it, p in zip(batch, parsed):
            norm = _normalize(p)
            it["language"] = norm["language"]
            it["sentiment"] = norm["sentiment"]
            it["topic_predefined"] = norm["topic_predefined"]
            it["topic_auto"] = norm["topic_auto"]

    return items
