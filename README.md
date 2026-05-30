# listenv1

A lightweight social listening dashboard for **Aza Fashions**. It scrapes brand mentions across Instagram, Facebook, Reddit, and TikTok, classifies each post for sentiment and topic using Gemini, and serves the results as a static dashboard on GitHub Pages.

**Live dashboard:** [prashantapurkayastha.github.io/listenv1](https://prashantapurkayastha.github.io/listenv1/)

## What it does

The pipeline runs daily on GitHub Actions and produces a dashboard with:

- **Volume over time** — daily mention counts across platforms
- **Sentiment breakdown** by platform (positive / neutral / negative)
- **Top positive and negative comments** with links to the source
- **Predefined topic buckets** (configured in `config/topics.json`)
- **Auto-discovered themes** surfaced from the corpus
- **Word clouds** for each sentiment class
- **Top subreddits** and **top commenters**

Filterable by time range (last 7 / 30 / 90 days or all time).

## How it works

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────┐
│  Apify actors   │ ──▶ │   scrapers/  │ ──▶ │  pipeline/  │ ──▶ │  data/   │
│ (IG, FB, RD,TT) │     │  (per-source)│     │ classify +  │     │  JSON    │
└─────────────────┘     └──────────────┘     │  aggregate  │     └────┬─────┘
                                              └─────────────┘          │
                                                     ▲                 ▼
                                              ┌──────┴──────┐    ┌──────────┐
                                              │ Gemini API  │    │ index.html│
                                              │ (classify)  │    │  + app.js │
                                              └─────────────┘    └──────────┘
```

1. **Scrape** — `scrapers/instagram.py`, `facebook.py`, `reddit.py`, `tiktok.py` each call an Apify actor and normalize the response.
2. **Filter** — Reddit hits go through `pipeline/filter.py`, which drops ambiguous brand-term matches (e.g. "aza" as a name vs. the brand) using the `strict` / `ambiguous` lists in `config/brand-terms.json`.
3. **Classify** — `pipeline/classify.py` sends new or changed items to Gemini for sentiment + topic-bucket assignment. Previously-seen items are skipped to save tokens.
4. **Store + merge** — `pipeline/store.py` writes per-day, per-platform JSON files under `data/` and merges with prior runs.
5. **Aggregate** — `pipeline/aggregate.py` builds the final `data/index.json` the frontend consumes.
6. **Serve** — `index.html` + `app.js` render charts (Chart.js) and word clouds (wordcloud2.js) directly from the JSON, hosted on GitHub Pages.

## Repository layout

```
├── .github/workflows/   # Scheduled GitHub Action that runs the pipeline
├── config/
│   ├── handles.json     # Per-platform accounts/handles to scrape
│   ├── topics.json      # Predefined topic buckets for classification
│   └── brand-terms.json # Strict + ambiguous brand-term lists (Reddit filter)
├── scrapers/            # One module per source (instagram, facebook, reddit, tiktok)
├── pipeline/
│   ├── classify.py      # Gemini-based sentiment + topic classifier
│   ├── filter.py        # Reddit ambiguity filter
│   ├── store.py         # Per-day JSON persistence + merge
│   └── aggregate.py     # Builds index.json for the frontend
├── data/                # Output JSON (per-platform per-day + index.json)
├── logs/                # Daily run logs
├── run.py               # Orchestrator — entry point
├── index.html           # Dashboard markup
├── app.js               # Dashboard logic (Chart.js + wordcloud2.js)
├── styles.css
└── requirements.txt
```

## Setup

### Prerequisites

- Python 3.10+
- An [Apify](https://apify.com/) account + API token (for IG / FB / Reddit / TikTok actors)
- A [Google AI Studio](https://aistudio.google.com/) API key for Gemini

### Install

```bash
git clone https://github.com/prashantapurkayastha/listenv1.git
cd listenv1
pip install -r requirements.txt
```

Dependencies are intentionally minimal:

- `apify-client` — driving the scraping actors
- `google-genai` — sentiment + topic classification
- `python-dateutil`

### Configure

Edit the three files in `config/`:

- **`handles.json`** — accounts to monitor on each platform
  ```json
  {
    "instagram": ["azafashions", "..."],
    "facebook":  ["azafashions"],
    "tiktok":    ["azafashions"]
  }
  ```
- **`topics.json`** — your topic buckets
  ```json
  { "buckets": ["product quality", "delivery", "pricing", "customer service", "..."] }
  ```
- **`brand-terms.json`** — used by the Reddit filter to disambiguate
  ```json
  {
    "strict":    ["aza fashions", "azafashions.com"],
    "ambiguous": ["aza"]
  }
  ```

### Run locally

```bash
export APIFY_TOKEN=your_apify_token
export GEMINI_API_KEY=your_gemini_key
python run.py
```

The script aborts early if either secret is missing. Output lands in `data/` and a timestamped log in `logs/`.

### Run automatically (GitHub Actions)

The workflow under `.github/workflows/` runs the pipeline on a schedule (daily at 01:00 UTC, per the footer note in `index.html`) and commits updated `data/` back to the repo. To enable it:

1. Add `APIFY_TOKEN` and `GEMINI_API_KEY` as repository secrets.
2. Enable GitHub Pages on the `main` branch.

## Frontend

The dashboard is intentionally dependency-light:

- Pure static HTML + vanilla JS — no build step
- [Chart.js](https://www.chartjs.org/) (4.x) for charts
- [wordcloud2.js](https://github.com/timdream/wordcloud2.js) for word clouds
- Light + dark mode via `prefers-color-scheme`
- "Refresh data" button re-fetches `data/index.json` without a full page reload

## Roadmap

Currently scoped to IG / FB / Reddit / TikTok. The code includes commented-out wiring for Twitter and Quora, parked for a v2.1.

## License

No license declared. Treat as source-available for reference unless the author specifies otherwise.
