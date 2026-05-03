"""
Check CTCL Facebook links using an authenticated Playwright session.

Flow:
  1. Opens a visible Chromium window at facebook.com
  2. You log in manually, then press Enter in the terminal
  3. Script processes all FBID rows headlessly using your session
  4. Extracts page title, classifies outcome, fuzzy-matches against office name

New columns added to CTCL_2025_link_check.csv:
  fb_page_title      - raw <title> text from the page
  fb_outcome         - live / not_found / login_wall / error
  fb_name_match      - fuzzy match score (0–100) against office name
  fb_name_flag       - True if score < MATCH_THRESHOLD or outcome != live

Usage:
    python3 check_ctcl_facebook_browser.py
"""

import asyncio
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from rapidfuzz import fuzz

HERE   = Path(__file__).parent
INPUT  = HERE / "data/CTCL_2025_link_check.csv"
OUTPUT = INPUT

CONCURRENCY     = 8
TIMEOUT_MS      = 15_000
MATCH_THRESHOLD = 60   # scores below this get flagged


def classify_outcome(page_name: str, final_url: str) -> str:
    """Classify based on the extracted page name and final URL."""
    u = final_url.lower()
    if "login" in u or "checkpoint" in u:
        return "login_wall"
    if not page_name:
        return "not_found"
    n = page_name.lower()
    if "page not found" in n or "content not found" in n or "this page isn't available" in n:
        return "not_found"
    return "live"


def match_score(page_name: str, office_name: str, jurisdiction: str) -> int:
    """Best fuzzy score against office name or jurisdiction."""
    if not page_name:
        return 0
    candidates = [str(office_name or ""), str(jurisdiction or "")]
    return max(fuzz.partial_ratio(page_name.lower(), c.lower()) for c in candidates if c)


async def check_url(context, url: str) -> tuple[str, str]:
    """Returns (final_url, page_name).
    Extracts the Facebook page name from the DOM (second h1, skipping 'Notifications'),
    which is more reliable than the tab title which includes notification counts.
    """
    page = await context.new_page()
    try:
        await page.goto(url, timeout=TIMEOUT_MS, wait_until="domcontentloaded")
        final_url = page.url
        page_name = await page.evaluate("""() => {
            const h1s = Array.from(document.querySelectorAll('h1'))
                             .map(el => el.innerText.trim())
                             .filter(t => t && t !== 'Notifications');
            return h1s[0] || '';
        }""")
        return final_url, page_name
    except PlaywrightTimeout:
        return url, ""
    except Exception:
        return url, ""
    finally:
        await page.close()


async def main(limit: int | None = None):
    df = pd.read_csv(INPUT)

    # Ensure result columns exist
    for col in ["fb_page_title", "fb_outcome", "fb_name_match", "fb_name_flag"]:
        if col not in df.columns:
            df[col] = None

    to_check = df[df["FBID"].notna()].copy()
    if limit:
        to_check = to_check.head(limit)
    print(f"Processing {len(to_check)} rows with a Facebook link.")

    async with async_playwright() as pw:
        # Step 1: visible browser for manual login
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()
        await page.goto("https://www.facebook.com")
        print("\nA browser window has opened. Please log into Facebook, then press Enter here to continue...")
        input()
        await page.close()

        # Step 2: headless crawl using the authenticated session
        print(f"Starting headless crawl of {len(to_check)} pages ({CONCURRENCY} concurrent)…")
        sem = asyncio.Semaphore(CONCURRENCY)
        results: dict[int, tuple[str, str]] = {}
        completed = 0

        async def process(idx, url):
            nonlocal completed
            async with sem:
                final_url, title = await check_url(context, url)
                results[idx] = (final_url, title)
                completed += 1
                if completed % 100 == 0:
                    print(f"  {completed}/{len(to_check)}")

        await asyncio.gather(*[
            process(idx, row["FBID"])
            for idx, row in to_check.iterrows()
        ])

        await context.close()
        await browser.close()

    # Apply results
    flagged = 0
    for idx, (final_url, page_name) in results.items():
        outcome = classify_outcome(page_name, final_url)
        score = match_score(page_name, df.at[idx, "Office Name"], df.at[idx, "[NEW] Jurisdictions"])
        flag = outcome != "live" or score < MATCH_THRESHOLD

        df.at[idx, "fb_page_title"] = page_name
        df.at[idx, "fb_outcome"]    = outcome
        df.at[idx, "fb_name_match"] = score
        df.at[idx, "fb_name_flag"]  = flag
        if flag:
            flagged += 1

    df.to_csv(OUTPUT, index=False)

    print(f"\nDone. {flagged} rows flagged → {OUTPUT}")
    print("\n=== Facebook outcome breakdown ===")
    print(df["fb_outcome"].value_counts(dropna=False).to_string())
    print(f"\n=== Name match score distribution (live pages only) ===")
    live = df[df["fb_outcome"] == "live"]["fb_name_match"]
    print(live.describe().to_string())


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N rows (for testing)")
    args = parser.parse_args()
    asyncio.run(main(limit=args.limit))
