"""
BPC PROJECT: Re-crawl the election office dataset to get current redirect URLs.

Reads LEO_combined_2024.csv (the canonical BPC dataset with is_primary_leo).
For each office, the crawl starts from the previous cycle's final destination
(--prev-redirects) rather than the original CTCL-listed URL. This avoids
false regressions where an office completed a .gov migration and retired their
old redirect — we follow where they actually are now, not where they used to be.

If no --prev-redirects file is given, falls back to the original CTCL URL.

Progress is saved every SAVE_INTERVAL rows so the script is safe to interrupt
and resume. Re-running will pick up where it left off.

Usage:
    python fetch_redirects.py --prev-redirects data/2024/LEO_combined_redirects_2024.csv
    python fetch_redirects.py   # falls back to original CTCL URLs
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
INPUT  = HERE / "data/2024/LEO_combined_2024.csv"
OUTPUT = HERE / "data/2026/LEO_combined_redirects_2026.csv"

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

def get_final_url(initial_url: str) -> str:
    """Follow redirects and return the final URL, or a sentinel on total failure."""
    parsed = urllib.parse.urlparse(initial_url)
    scheme = parsed.scheme or "http"
    domain = parsed.netloc or parsed.path.split("/")[0]
    path = parsed.path if parsed.netloc else "/" + "/".join(parsed.path.split("/")[1:])
    if path and not path.startswith("/"):
        path = "/" + path

    other_scheme = "https" if scheme == "http" else "http"
    candidates = list(dict.fromkeys([
        initial_url,
        f"{scheme}://{domain}{path}",
        f"{scheme}://www.{domain}{path}",
        f"{other_scheme}://{domain}{path}",
        f"{other_scheme}://www.{domain}{path}",
    ]))

    for url in candidates:
        for _ in range(MAX_RETRIES):
            try:
                r = requests.get(
                    url, allow_redirects=True,
                    timeout=TIMEOUT, headers=HEADERS, verify=False
                )
                return r.url
            except (SSLError, Timeout, RequestException):
                continue

    return "All endpoints failed"


def normalize_url(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    p = urllib.parse.urlparse(url)
    netloc = p.netloc.removeprefix("www.")
    norm = netloc + p.path.rstrip("/")
    if p.query:
        norm += "?" + p.query
    return norm.lower()


def get_netloc(url: str, remove_www: bool = True) -> str:
    if not isinstance(url, str):
        return ""
    netloc = urllib.parse.urlparse(url).netloc
    if remove_www and netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def classify_status(website: str, redirect: str) -> str:
    if not isinstance(website, str):
        return "no website"
    if redirect == "All endpoints failed":
        return "error"
    if website == redirect or normalize_url(website) == normalize_url(redirect):
        return "same website"
    if get_netloc(website) != get_netloc(redirect):
        return "different netloc"
    return "same netloc"


# ── main ───────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--prev-redirects", type=Path, default=None,
                        help="Previous cycle's redirects CSV; its website_redirect column "
                             "is used as the starting URL instead of the original CTCL URL.")
    args = parser.parse_args()

    df = pd.read_csv(INPUT)

    # Use previous cycle's final destinations as starting URLs where available
    if args.prev_redirects:
        prev = pd.read_csv(args.prev_redirects)[["Office UUID", "website_redirect"]]
        prev = prev[prev["website_redirect"].notna() & (prev["website_redirect"] != "All endpoints failed")]
        prev = prev.rename(columns={"website_redirect": "website_prev"})
        df = df.merge(prev, on="Office UUID", how="left")
        df["website_start"] = df["website_prev"].fillna(df["website"])
        df = df.drop(columns=["website_prev"])
        print(f"Using previous redirects for {prev['Office UUID'].isin(df['Office UUID']).sum()} rows.")
    else:
        df["website_start"] = df["website"]

    # Resume from partial output if it exists
    if OUTPUT.exists():
        done = pd.read_csv(OUTPUT)
        done_uuids = set(done["Office UUID"])
        remaining = df[~df["Office UUID"].isin(done_uuids)].copy()
        print(f"Resuming: {len(done_uuids)} already done, {len(remaining)} remaining.")
    else:
        done = pd.DataFrame()
        remaining = df.copy()
        remaining["website_redirect"] = None
        remaining["website_status"] = None

    pending = remaining[remaining["website_start"].notna()].index
    print(f"Crawling {len(pending)} URLs (skipping {remaining['website_start'].isna().sum()} rows with no website) …")

    for i, idx in enumerate(tqdm(pending)):
        url = remaining.at[idx, "website_start"]
        try:
            redirect = get_final_url(url)
        except Exception as e:
            redirect = f"Error: {type(e).__name__}"
        remaining.at[idx, "website_redirect"] = redirect
        remaining.at[idx, "website_status"] = classify_status(remaining.at[idx, "website"], redirect)

        if (i + 1) % SAVE_INTERVAL == 0:
            out = pd.concat([done, remaining], ignore_index=True) if not done.empty else remaining
            out.to_csv(OUTPUT, index=False)

    out = pd.concat([done, remaining], ignore_index=True) if not done.empty else remaining
    out.to_csv(OUTPUT, index=False)
    print(f"Done. {len(out)} rows → {OUTPUT}")


if __name__ == "__main__":
    main()
