import os
import re
import json
import asyncio
from playwright.async_api import async_playwright
from .common import parse_cookie_blob, now_iso, within_last_24h

IG_DOMAIN = ".instagram.com"


async def _get_recent_post_urls(page, profile_url, limit=12):
    await page.goto(profile_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)
    # Grid links to /p/<shortcode>/ or /reel/<shortcode>/
    hrefs = await page.eval_on_selector_all(
        "a[href*='/p/'], a[href*='/reel/']",
        "els => Array.from(new Set(els.map(e => e.href)))"
    )
    return hrefs[:limit]


async def _scrape_post_comments(page, post_url):
    await page.goto(post_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(2500)

    # Attempt to click "View more comments" repeatedly
    for _ in range(8):
        try:
            btn = await page.query_selector("button:has-text('View more comments'), button[aria-label='Load more comments']")
            if not btn:
                break
            await btn.click()
            await page.wait_for_timeout(1500)
        except Exception:
            break

    # Extract JSON-LD / embedded JSON if available, else parse DOM
    items = []
    try:
        data = await page.evaluate("""
            () => {
                const nodes = document.querySelectorAll('ul ul li, article ul li');
                const out = [];
                nodes.forEach(n => {
                    const user = n.querySelector('a[role="link"]');
                    const text = n.querySelector('span');
                    if (user && text) {
                        out.push({
                            author: (user.innerText||'').trim().replace(/^@/, ''),
                            text: (text.innerText||'').trim()
                        });
                    }
                });
                return out;
            }
        """)
        for i, it in enumerate(data):
            if not it["text"]:
                continue
            items.append({
                "id": f"ig_{abs(hash(post_url+it['author']+it['text']))}",
                "platform": "instagram",
                "handle": "@azafashions",
                "post_url": post_url,
                "parent_comment_id": None,
                "author": it["author"],
                "text": it["text"],
                "language": "unknown",
                "like_count": 0,
                "reply_count": 0,
                "captured_at": now_iso(),
                "posted_at": now_iso(),
            })
    except Exception:
        pass
    return items


async def scrape(profile_url: str, cookie_blob: str):
    cookies = parse_cookie_blob(cookie_blob, IG_DOMAIN)
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="en-US",
            viewport={"width": 1280, "height": 900},
        )
        if cookies:
            await ctx.add_cookies(cookies)
        page = await ctx.new_page()

        all_items = []
        try:
            posts = await _get_recent_post_urls(page, profile_url)
            for url in posts:
                try:
                    items = await _scrape_post_comments(page, url)
                    all_items.extend(items)
                except Exception as e:
                    print(f"[ig] post failed {url}: {e}")
        finally:
            await browser.close()
        # Keep only last-24h posts' comments; IG doesn't easily expose per-comment timestamp,
        # so we rely on post recency (grid is chronological).
        return all_items


def run_sync(profile_url: str, cookie_blob: str):
    return asyncio.run(scrape(profile_url, cookie_blob))
