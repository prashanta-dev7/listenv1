import os
import json
import time
from anthropic import Anthropic

MODEL = "claude-3-5-sonnet-latest"
BATCH_SIZE = 20


def _client():
    return Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


def _build_prompt(items, buckets):
    bucket_lines = "\n".join(f"- {b['id']}: {b['description']}" for b in buckets)
    item_lines = "\n".join(f"{i}. {json.dumps(it['text'])}" for i, it in enumerate(items))
    return f"""You are classifying social media comments for a fashion brand.

For EACH numbered comment below, return a JSON object with:
- "language": "english" or "non-english"
- "sentiment": "positive", "negative", or "neutral"  (only if english; else null)
- "topic_predefined": one of [{", ".join(b['id'] for b in buckets)}] or null
- "topic_auto": a short 2-4 word free-form theme label (english only; else null)

Predefined buckets:
{bucket_lines}

Return ONLY a JSON array of length {len(items)}, in order. No prose.

Comments:
{item_lines}
"""


def _parse_response(text, n):
    # Strip code fences if present
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.lower().startswith("json"):
            t = t[4:]
    start = t.find("[")
    end = t.rfind("]")
    if start == -1 or end == -1:
        return [{} for _ in range(n)]
    try:
        arr = json.loads(t[start:end + 1])
    except Exception:
        return [{} for _ in range(n)]
    if len(arr) != n:
        # pad/truncate
        arr = (arr + [{}] * n)[:n]
    return arr


def classify(items, buckets):
    """Mutates each item in-place with language/sentiment/topic fields."""
    if not items:
        return items
    client = _client()
    for start in range(0, len(items), BATCH_SIZE):
        batch = items[start:start + BATCH_SIZE]
        prompt = _build_prompt(batch, buckets)
        for attempt in range(3):
            try:
                resp = client.messages.create(
                    model=MODEL,
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = resp.content.text
                parsed = _parse_response(text, len(batch))
                for it, p in zip(batch, parsed):
                    it["language"] = p.get("language") or "unknown"
                    if it["language"] == "english":
                        it["sentiment"] = p.get("sentiment") or "neutral"
                    else:
                        it["sentiment"] = None
                    it["topic_predefined"] = p.get("topic_predefined")
                    it["topic_auto"] = p.get("topic_auto")
                break
            except Exception as e:
                print(f"[classify] attempt {attempt+1} failed: {e}")
                time.sleep(2 ** attempt)
        else:
            # Give up for this batch; mark unknown
            for it in batch:
                it.setdefault("language", "unknown")
                it.setdefault("sentiment", None)
                it.setdefault("topic_predefined", None)
                it.setdefault("topic_auto", None)
    return items
