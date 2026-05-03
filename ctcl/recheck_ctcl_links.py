"""
Re-check CTCL link rows that previously came back dead or errored.
Updates the status in place only if the new result is better.

Usage:
    python recheck_ctcl_links.py
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
INPUT  = HERE / "data/CTCL_2025_link_check.csv"
OUTPUT = INPUT  # update in place

SAVE_INTERVAL = 100
MAX_RETRIES   = 2
TIMEOUT       = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/39.0.2171.95 Safari/537.36"
    )
}

STATUS_RANK = {"live": 0, "redirected": 1, "http_error": 2, "dead": 3}


def fetch(url: str) -> tuple[str | None, int | None]:
    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(url, allow_redirects=True,
                             timeout=TIMEOUT, headers=HEADERS, verify=False)
            return r.url, r.status_code
        except (SSLError, Timeout, RequestException):
            continue
    return None, None


def get_netloc(url: str) -> str:
    if not isinstance(url, str): return ""
    return urllib.parse.urlparse(url).netloc.removeprefix("www.")


def classify_website(original: str, final_url: str | None, status: int | None) -> str:
    if final_url is None:
        return "dead"
    if status is not None and status >= 400:
        return "http_error"
    if get_netloc(original) != get_netloc(final_url):
        return "redirected"
    return "live"


def is_better(new_status: str, old_status: str) -> bool:
    return STATUS_RANK.get(new_status, 99) < STATUS_RANK.get(old_status, 99)


def main():
    df = pd.read_csv(INPUT)

    website_mask = df["website_status"].isin(["dead", "http_error"])
    fb_mask      = df["fb_status"] == "dead"
    to_recheck   = df[website_mask | fb_mask].copy()

    print(f"Re-checking {website_mask.sum()} website rows and {fb_mask.sum()} Facebook rows "
          f"({len(to_recheck)} total, some overlap) …")

    for i, (idx, row) in enumerate(tqdm(to_recheck.iterrows(), total=len(to_recheck))):

        # Website
        if website_mask.loc[idx] and pd.notna(row.get("Website")):
            final_url, status_code = fetch(row["Website"])
            new_status = classify_website(row["Website"], final_url, status_code)
            if is_better(new_status, df.at[idx, "website_status"]):
                df.at[idx, "website_final_url"]   = final_url
                df.at[idx, "website_status_code"] = status_code
                df.at[idx, "website_status"]      = new_status

        # Facebook
        if fb_mask.loc[idx] and pd.notna(row.get("FBID")):
            final_url, status_code = fetch(row["FBID"])
            new_fb = "dead" if final_url is None else ("http_error" if (status_code and status_code >= 400) else "live")
            if is_better(new_fb, df.at[idx, "fb_status"]):
                df.at[idx, "fb_status"] = new_fb

        if (i + 1) % SAVE_INTERVAL == 0:
            df.to_csv(OUTPUT, index=False)

    df.to_csv(OUTPUT, index=False)

    print(f"\nDone. Updated {OUTPUT}")
    print("\n=== Website status (after recheck) ===")
    print(df["website_status"].value_counts(dropna=False).to_string())
    print("\n=== Facebook status (after recheck) ===")
    print(df["fb_status"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
