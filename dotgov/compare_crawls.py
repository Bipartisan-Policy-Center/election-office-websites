"""
compare_crawls.py — quantify movement between the May 2026 crawl and the refreshed
re-crawl, among primary LEOs with a listed website. Supports the Methods writeup
("how much did the refresh change") and surfaces any regressions for review.

Reads data/2026/recrawl_2026.csv, which carries BOTH:
  - website_redirect      (May 2026 final URL)
  - recrawl_final_url      (refreshed final URL) + outcome
"""
from pathlib import Path

import pandas as pd
from tldextract import extract as tld

HERE = Path(__file__).resolve().parent
RECRAWL = HERE / "data/2026/recrawl_2026.csv"


def isgov(x) -> bool:
    if not isinstance(x, str) or not x:
        return False
    if x.startswith("http"):
        return tld(x).suffix == "gov"
    return x.rstrip("/").endswith(".gov")


def main() -> None:
    d = pd.read_csv(RECRAWL, low_memory=False)
    p = d[(d["is_primary_leo"] == True) & d["website"].notna()].copy()

    p["may_gov"] = p["website_redirect"].apply(isgov)
    p["new_gov"] = (p["outcome"] == "gov")
    p["new_unreachable"] = p["outcome"].isin(["dead", "transient"])

    n = len(p)
    print(f"Primary LEOs with a listed website: {n}\n")

    print("=== .gov count ===")
    print(f"  May crawl:        {int(p['may_gov'].sum())}")
    print(f"  Refreshed crawl:  {int(p['new_gov'].sum())}")
    print(f"  Net change:       {int(p['new_gov'].sum() - p['may_gov'].sum()):+d}\n")

    gained = p[~p["may_gov"] & p["new_gov"]]
    lost   = p[p["may_gov"] & ~p["new_gov"]]
    print("=== transitions ===")
    print(f"  non-.gov/error -> .gov (gained):  {len(gained)}")
    print(f"  .gov -> non-.gov/unreachable (regressions): {len(lost)}")
    print(f"  still unreachable (dead/transient): {int(p['new_unreachable'].sum())}\n")

    if len(lost):
        print("=== REGRESSIONS to review (was .gov in May, not now) ===")
        cols = ["Office UUID", "State", "Jurisdiction", "outcome",
                "website_redirect", "recrawl_final_url"]
        print(lost[cols].to_string(index=False))


if __name__ == "__main__":
    main()
