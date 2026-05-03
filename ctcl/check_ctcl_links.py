"""
CTCL PROJECT: Check all website and Facebook links in the 2025 LEOD for broken/dead links.

For each office in the 2025 dataset:
  - Website: follow redirects, classify as live / redirected / dead
  - Facebook (FBID): check whether the page is still accessible

Output: data/CTCL_2025_link_check.csv

Progress is saved every SAVE_INTERVAL rows.

Usage:
    python check_ctcl_links.py
"""

import urllib.parse
import warnings
from pathlib import Path

import pandas as pd
import requests
from requests.exceptions import RequestException, SSLError, Timeout
from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter("ignore", InsecureRequestWarning)

# ── paths ──────────────────────────────────────────────────────────────────
HERE   = Path(__file__).parent
INPUT  = HERE / "data/from_ctcl_202603/EXTERNAL LEOD_combined_2025.csv"
OUTPUT = HERE / "data/CTCL_2025_link_check.csv"

SAVE_INTERVAL = 200
MAX_RETRIES   = 2
TIMEOUT       = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/39.0.2171.95 Safari/537.36"
    )
}


# ── URL helpers ────────────────────────────────────────────────────────────

def fetch(url: str) -> tuple[str | None, int | None]:
    """Return (final_url, status_code) or (None, None) on total failure."""
    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(
                url, allow_redirects=True,
                timeout=TIMEOUT, headers=HEADERS, verify=False
            )
            return r.url, r.status_code
        except (SSLError, Timeout, RequestException):
            continue
    return None, None


def get_netloc(url: str) -> str:
    if not isinstance(url, str):
        return ""
    netloc = urllib.parse.urlparse(url).netloc
    return netloc.removeprefix("www.")


def classify_website(original: str, final_url: str | None, status: int | None) -> str:
    """
    live           – reachable, same domain
    redirected     – reachable, but landed on a different domain
    dead           – unreachable (connection failed entirely)
    http_error     – reachable but returned 4xx/5xx
    """
    if final_url is None:
        return "dead"
    if status is not None and status >= 400:
        return "http_error"
    if get_netloc(original) != get_netloc(final_url):
        return "redirected"
    return "live"


def check_facebook(fbid_url: str) -> str:
    """
    Returns: live / dead / http_error
    Facebook aggressively blocks bots, so a non-200 is ambiguous — we flag it
    but callers should treat it as 'needs manual review' rather than confirmed dead.
    """
    if not isinstance(fbid_url, str) or not fbid_url.strip():
        return ""
    final_url, status = fetch(fbid_url)
    if final_url is None:
        return "dead"
    if status is not None and status >= 400:
        return "http_error"
    return "live"


# ── main ───────────────────────────────────────────────────────────────────

def main():
    df = pd.read_csv(INPUT, encoding="utf-8-sig")
    df = df.rename(columns={"Office UUID (maps to govproj)": "Office UUID"})

    # Resume if output already exists
    if OUTPUT.exists():
        done = pd.read_csv(OUTPUT)
        done_uuids = set(done["Office UUID"])
        df = df[~df["Office UUID"].isin(done_uuids)].copy()
        print(f"Resuming: {len(done_uuids)} done, {len(df)} remaining.")
    else:
        done = pd.DataFrame()

    # Columns we'll fill in
    for col in ["website_final_url", "website_status_code", "website_status",
                "fb_status"]:
        if col not in df.columns:
            df[col] = None

    website_pending = df["Website"].notna()
    fb_pending = df["FBID"].notna()

    print(f"Checking {website_pending.sum()} website URLs and {fb_pending.sum()} Facebook URLs …")

    for i, idx in enumerate(tqdm(df.index)):
        row = df.loc[idx]

        # Website
        if pd.notna(row.get("Website")):
            final_url, status = fetch(row["Website"])
            df.at[idx, "website_final_url"] = final_url
            df.at[idx, "website_status_code"] = status
            df.at[idx, "website_status"] = classify_website(row["Website"], final_url, status)

        # Facebook
        if pd.notna(row.get("FBID")):
            df.at[idx, "fb_status"] = check_facebook(row["FBID"])

        if (i + 1) % SAVE_INTERVAL == 0:
            out = pd.concat([done, df], ignore_index=True) if not done.empty else df
            out.to_csv(OUTPUT, index=False)

    out = pd.concat([done, df], ignore_index=True) if not done.empty else df
    out.to_csv(OUTPUT, index=False)

    # Summary
    print(f"\nDone. Saved {len(out)} rows → {OUTPUT}")
    print("\n=== Website link status ===")
    print(out["website_status"].value_counts(dropna=False).to_string())
    print("\n=== Facebook link status ===")
    print(out["fb_status"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
