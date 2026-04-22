import asyncio
from playwright.async_api import async_playwright
from .common import parse_cookie_blob, now_iso

FB_DOMAIN = ".facebook.com"


async def _get_recent_post_urls(page, profile_url, limit=10):
    await page.goto(profile_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(3500)
    # Scroll a bit to load recent posts
    for _ in range(4):
        await page.mouse.wheel(0, 2500)
        await page.wait_for_timeout(1200)
    hrefs = await page.eval_on_selector_all(
        "a[href*='/posts/'], a[href*='/videos/'], a[href*='/reel/']",
        "els => Array.from(new Set(els.map(e => e.href.split('?')[0])))"
    )
    return hrefs[:limit]


async def _scrape_post_comments(page, post_url):
    await page.goto(post_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)

    # Try to expand "View more comments" / "All comments"
    for _ in range(10):
        try:
            btn = await page.query_selector(
                "div[role='button']:has-text('View more comments'), "
                "div[role='button']:has-text('All comments'), "
                "div[role='button']:has-text('Most relevant')"
            )
            if not btn:
                break
            await btn.click()
            await page.wait_for_timeout(1500)
        except Exception:
            break

    items = await page.evaluate("""
        () => {
            const out = [];
            document.querySelectorAll("div[aria-label^='Comment by']").forEach(node => {
                const label = node.getAttribute('aria-label') || '';
                const author = (label.replace(/^Comment by /,'').split(':')[0] || '').trim();
                const text = (node.innerText || '').trim();
                if (text) out.push({ author, text });
            });
            return out;
        }
    """)

    out = []
    for it in items:
        out.append({
            "id": f"fb_{abs(hash(post_url+it['author']+it['text']))}",
            "platform": "facebook",
            "handle": "@AzaFashions",
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
    return out


async def scrape(profile_url: str, cookie_blob: str):
    cookies = parse_cookie_blob(cookie_blob, FB_DOMAIN)
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
                    print(f"[fb] post failed {url}: {e}")
        finally:
            await browser.close()
        return all_items


def run_sync(profile_url: str, cookie_blob: str):
    return asyncio.run(scrape(profile_url, cookie_blob))
