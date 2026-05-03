"""
Re-check http_error rows from CTCL_2025_link_check.csv using a real Chromium
browser (Playwright), which bypasses most bot-blocking 403s.

Updates website_status in place only if the new result is better:
  live < redirected < http_error < dead  (lower rank = better)

Usage:
    python3 recheck_ctcl_links_browser.py
"""

import asyncio
import urllib.parse
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

HERE   = Path(__file__).parent
INPUT  = HERE / "data/CTCL_2025_link_check.csv"
OUTPUT = INPUT

CONCURRENCY  = 10
TIMEOUT_MS   = 12_000
SAVE_INTERVAL = 100

STATUS_RANK = {"live": 0, "redirected": 1, "http_error": 2, "dead": 3}


def get_netloc(url: str) -> str:
    if not isinstance(url, str):
        return ""
    return urllib.parse.urlparse(url).netloc.removeprefix("www.")


def classify(original: str, final_url: str | None, status: int | None) -> str:
    if final_url is None:
        return "dead"
    if status is not None and status >= 400:
        return "http_error"
    if get_netloc(original) != get_netloc(final_url):
        return "redirected"
    return "live"


def is_better(new: str, old: str) -> bool:
    return STATUS_RANK.get(new, 99) < STATUS_RANK.get(old, 99)


async def check_url(context, original_url: str) -> tuple[str | None, int | None]:
    page = await context.new_page()
    try:
        resp = await page.goto(original_url, timeout=TIMEOUT_MS, wait_until="domcontentloaded")
        final_url = page.url
        status = resp.status if resp else None
        return final_url, status
    except PlaywrightTimeout:
        return None, None
    except Exception:
        return None, None
    finally:
        await page.close()


async def main():
    df = pd.read_csv(INPUT)

    mask = df["website_status"] == "http_error"
    to_recheck = df[mask].copy()
    print(f"Re-checking {len(to_recheck)} http_error rows with Chromium …")

    sem = asyncio.Semaphore(CONCURRENCY)
    results: dict[int, tuple[str | None, int | None]] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            ignore_https_errors=True,
        )

        completed = 0

        async def process(idx, url):
            nonlocal completed
            async with sem:
                final_url, status = await check_url(context, url)
                results[idx] = (final_url, status)
                completed += 1
                if completed % 50 == 0:
                    print(f"  {completed}/{len(to_recheck)}")

        tasks = [
            process(idx, row["Website"])
            for idx, row in to_recheck.iterrows()
            if pd.notna(row.get("Website"))
        ]
        await asyncio.gather(*tasks)

        await context.close()
        await browser.close()

    # Apply results back to df
    improved = 0
    for idx, (final_url, status) in results.items():
        original = df.at[idx, "Website"]
        new_status = classify(original, final_url, status)
        if is_better(new_status, df.at[idx, "website_status"]):
            df.at[idx, "website_final_url"] = final_url
            df.at[idx, "website_status_code"] = status
            df.at[idx, "website_status"] = new_status
            improved += 1

    df.to_csv(OUTPUT, index=False)
    print(f"\nDone. {improved} rows improved → {OUTPUT}")
    print("\n=== Website status (after browser recheck) ===")
    print(df["website_status"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    asyncio.run(main())
