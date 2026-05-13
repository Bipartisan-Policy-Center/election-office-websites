"""
One-time patch: re-crawl the 697 rows in LEO_combined_redirects_2026.csv where
the 2024 final destination differs from what we crawled in 2026 (because we
started from the original CTCL URL rather than the 2024 final destination).

Updates LEO_combined_redirects_2026.csv in place.

Usage:
    python patch_redirects_2026.py
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

HERE    = Path(__file__).parent
FILE24  = HERE / "data/2024/LEO_combined_redirects_2024.csv"
FILE26  = HERE / "data/2026/LEO_combined_redirects_2026.csv"

SAVE_INTERVAL = 50
MAX_RETRIES   = 2
TIMEOUT       = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/39.0.2171.95 Safari/537.36"
    )
}


def normalize(url: str) -> str:
    if not isinstance(url, str):
        return ""
    p = urllib.parse.urlparse(url.rstrip("/"))
    return f"{p.scheme}://{p.netloc.removeprefix('www.')}{p.path}".lower()


def get_netloc(url: str, remove_www: bool = True) -> str:
    if not isinstance(url, str):
        return ""
    netloc = urllib.parse.urlparse(url).netloc
    if remove_www and netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def normalize_url(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    p = urllib.parse.urlparse(url)
    netloc = p.netloc.removeprefix("www.")
    norm = netloc + p.path.rstrip("/")
    if p.query:
        norm += "?" + p.query
    return norm.lower()


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
    df24 = pd.read_csv(FILE24)
    df26 = pd.read_csv(FILE26)

    merged = df26.merge(
        df24[["Office UUID", "website_redirect"]],
        on="Office UUID", suffixes=("_2026", "_2024")
    )

    # Rows where 2024 final destination is a valid URL and differs from original
    valid_prev = (
        merged["website_redirect_2024"].notna() &
        (merged["website_redirect_2024"] != "All endpoints failed") &
        (merged["website_redirect_2024"] != merged["website"])
    )
    # And where the 2026 result doesn't already match the 2024 final destination
    result_differs = merged["norm_2024"] if "norm_2024" in merged.columns else merged["website_redirect_2024"].apply(normalize)
    merged["norm_2024"] = merged["website_redirect_2024"].apply(normalize)
    merged["norm_2026"] = merged["website_redirect_2026"].apply(normalize)
    needs_patch = valid_prev & (merged["norm_2024"] != merged["norm_2026"])

    to_patch = merged[needs_patch].copy()
    print(f"Patching {len(to_patch)} rows by crawling from 2024 final destination …")

    patch_results = {}
    for i, (_, row) in enumerate(tqdm(to_patch.iterrows(), total=len(to_patch))):
        start_url = row["website_redirect_2024"]
        redirect = get_final_url(start_url)
        patch_results[row["Office UUID"]] = redirect
        if (i + 1) % SAVE_INTERVAL == 0:
            _apply_and_save(df26, patch_results)

    _apply_and_save(df26, patch_results)
    print(f"Done. Updated {len(patch_results)} rows → {FILE26}")


def _apply_and_save(df26: pd.DataFrame, patch_results: dict):
    uuid_to_idx = df26.set_index("Office UUID").index
    for uuid, redirect in patch_results.items():
        mask = df26["Office UUID"] == uuid
        original_website = df26.loc[mask, "website"].iloc[0]
        df26.loc[mask, "website_redirect"] = redirect
        df26.loc[mask, "website_status"] = classify_status(original_website, redirect)
    df26.to_csv(FILE26, index=False)


if __name__ == "__main__":
    main()
