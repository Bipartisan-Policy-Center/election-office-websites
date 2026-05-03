"""
BPC PROJECT: Re-crawl the election office dataset to get current redirect URLs.

Reads data/LEO_combined.csv (same dataset used for the 2022→2024 analysis,
with is_primary_leo and other fields intact).

Every URL is visited fresh — we can't know whether a site now redirects to .gov
without checking, even if the listed URL hasn't changed.

Progress is saved every SAVE_INTERVAL rows so the script is safe to interrupt
and resume. Re-running will pick up where it left off.

Output: data/LEO_combined_with_redirects_2026.csv

Usage:
    python fetch_redirects.py
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
    df = pd.read_csv(INPUT)

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

    pending = remaining[remaining["website"].notna()].index
    print(f"Crawling {len(pending)} URLs (skipping {remaining['website'].isna().sum()} rows with no website) …")

    for i, idx in enumerate(tqdm(pending)):
        url = remaining.at[idx, "website"]
        try:
            redirect = get_final_url(url)
        except Exception as e:
            redirect = f"Error: {type(e).__name__}"
        remaining.at[idx, "website_redirect"] = redirect
        remaining.at[idx, "website_status"] = classify_status(url, redirect)

        if (i + 1) % SAVE_INTERVAL == 0:
            out = pd.concat([done, remaining], ignore_index=True) if not done.empty else remaining
            out.to_csv(OUTPUT, index=False)

    out = pd.concat([done, remaining], ignore_index=True) if not done.empty else remaining
    out.to_csv(OUTPUT, index=False)
    print(f"Done. {len(out)} rows → {OUTPUT}")


if __name__ == "__main__":
    main()
