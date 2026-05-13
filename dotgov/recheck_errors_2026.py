"""
One-time recheck: re-crawl the 548 rows in LEO_combined_redirects_2026.csv
where the crawl returned "All endpoints failed" — these may have been
temporarily unreachable and could now resolve correctly.

Updates LEO_combined_redirects_2026.csv in place, but ONLY if the new result
is better than "All endpoints failed" (i.e. we don't overwrite a success with
a new failure).

Usage:
    python recheck_errors_2026.py
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

HERE   = Path(__file__).parent
FILE26 = HERE / "data/2026/LEO_combined_redirects_2026.csv"

SAVE_INTERVAL = 50
MAX_RETRIES   = 3   # one extra retry vs. original crawl
TIMEOUT       = 8   # slightly longer timeout

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/39.0.2171.95 Safari/537.36"
    )
}


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


def get_final_url(initial_url: str) -> str:
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
                r = requests.get(url, allow_redirects=True,
                                 timeout=TIMEOUT, headers=HEADERS, verify=False)
                return r.url
            except (SSLError, Timeout, RequestException):
                continue
    return "All endpoints failed"


def classify_status(original_website: str, redirect: str) -> str:
    if not isinstance(original_website, str):
        return "no website"
    if redirect == "All endpoints failed":
        return "error"
    if original_website == redirect or normalize_url(original_website) == normalize_url(redirect):
        return "same website"
    if get_netloc(original_website) != get_netloc(redirect):
        return "different netloc"
    return "same netloc"


def main():
    df = pd.read_csv(FILE26)

    to_recheck = df[df["website_redirect"] == "All endpoints failed"].copy()
    # Only recheck rows that have a website to crawl from
    to_recheck = to_recheck[to_recheck["website"].notna()]
    print(f"Rechecking {len(to_recheck)} 'All endpoints failed' rows …")

    results = {}
    improved = 0

    for i, (_, row) in enumerate(tqdm(to_recheck.iterrows(), total=len(to_recheck))):
        redirect = get_final_url(row["website"])
        if redirect != "All endpoints failed":
            results[row["Office UUID"]] = (redirect, classify_status(row["website"], redirect))
            improved += 1

        if (i + 1) % SAVE_INTERVAL == 0 and results:
            _apply_and_save(df, results)
            print(f"  [{i+1}/{len(to_recheck)}] {improved} improved so far …")

    if results:
        _apply_and_save(df, results)

    print(f"\nDone. {improved}/{len(to_recheck)} rows recovered → {FILE26}")


def _apply_and_save(df: pd.DataFrame, results: dict):
    for uuid, (redirect, status) in results.items():
        mask = df["Office UUID"] == uuid
        df.loc[mask, "website_redirect"] = redirect
        df.loc[mask, "website_status"] = status
    df.to_csv(FILE26, index=False)


if __name__ == "__main__":
    main()
