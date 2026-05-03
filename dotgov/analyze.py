"""
.gov adoption analysis for local election office websites.

Reads the crawled redirect data (output of fetch_redirects.py) and produces:
  - Summary statistics (console)
  - Year-over-year comparison table (CSV) for import into Datawrapper

Usage:
    python analyze.py
"""

import re
import urllib.parse
from pathlib import Path

import pandas as pd
from tldextract import extract as tld

# ── paths ──────────────────────────────────────────────────────────────────
HERE           = Path(__file__).parent
REDIRECTS_2026 = HERE / "data/2026/LEO_combined_redirects_2026.csv"
COUNTY_ADJ     = HERE / "data/reference/county_adjacency2023.txt"

# ── helpers ────────────────────────────────────────────────────────────────

def isgov(x: str) -> bool | float:
    if not isinstance(x, str) or not x:
        return float("nan")
    if x.startswith("http"):
        return tld(x).suffix == "gov"
    return x.endswith(".gov")


def get_netloc(url, remove_www: bool = True) -> str:
    if not isinstance(url, str):
        return ""
    netloc = urllib.parse.urlparse(url).netloc
    if remove_www and netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def remove_port(url: str) -> str:
    return re.sub(r":\d+$", "", url)


def load_county_equivalents() -> pd.DataFrame:
    counties = pd.read_csv(COUNTY_ADJ, sep="|")[["County Name"]]
    counties["county_equivalent"] = True
    counties[["County_name", "State"]] = counties["County Name"].str.rsplit(", ", n=1, expand=True)
    counties = counties[counties["State"] != "PR"].drop(columns=["County Name"])
    return counties.drop_duplicates().reset_index(drop=True)


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["website"])
    # Deduplicate by netloc, matching the original methodology:
    # sort so primary LEOs and county-equivalents sort last (keep='last'),
    # so they win when multiple offices share the same final domain.
    df = df.copy()
    df["_primary"] = df["is_primary_leo"].fillna(False)
    df["_county"]  = df["county_equivalent"].fillna(False)
    df = df.sort_values(["_primary", "_county"])
    df = df.drop_duplicates(subset="netloc", keep="last")
    df = df.drop(columns=["_primary", "_county"])
    return df


def pct(num: int, den: int) -> str:
    return f"{num}/{den} ({num/den*100:.1f}%)"


# ── 20 most populous counties ──────────────────────────────────────────────
TOP_20 = [
    ("Los Angeles", "CA"), ("Cook", "IL"), ("Harris", "TX"),
    ("Maricopa", "AZ"), ("San Diego", "CA"), ("Orange", "CA"),
    ("Miami-Dade", "FL"), ("Kings", "NY"), ("Dallas", "TX"),
    ("Riverside", "CA"), ("Queens", "NY"), ("Clark", "NV"),
    ("King", "WA"), ("San Bernardino", "CA"), ("Tarrant", "TX"),
    ("Bexar", "TX"), ("Broward", "FL"), ("Santa Clara", "CA"),
    ("Wayne", "MI"), ("Alameda", "CA"),
]


def get_top_counties(df: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for county, state in TOP_20:
        mask = (
            (df["State"] == state) &
            (df["Office Name"].str.contains("County", na=False)) &
            (df["County"] == county)
        )
        frames.append(df[mask])
    # NYC: all 5 boroughs are served by one Board of Elections row whose County
    # field contains "Bronx, Kings, New York, Queens, Richmond"
    frames.append(df[(df["State"] == "NY") & df["County"].str.contains("Bronx", na=False) & (df["is_primary_leo"] == True)])
    result = pd.concat(frames).drop_duplicates("netloc")
    return result


# ── main ───────────────────────────────────────────────────────────────────

def main():
    if not REDIRECTS_2026.exists():
        raise FileNotFoundError(
            f"{REDIRECTS_2026} not found — run fetch_redirects.py first."
        )

    df = pd.read_csv(REDIRECTS_2026)
    counties = load_county_equivalents()

    # Merge county-equivalent flag
    df = df.merge(
        counties,
        left_on=[df["Jurisdiction"].str.lower(), "State"],
        right_on=[counties["County_name"].str.lower(), "State"],
        how="left",
        suffixes=("", "_county"),
    )
    df = df.drop(columns=["key_0", "County_name"], errors="ignore")

    # Compute netloc and isgov
    df["netloc"] = df["website_redirect"].apply(
        lambda x: get_netloc(x, remove_www=False)
    ).apply(remove_port)
    df["isgov"] = df["netloc"].apply(isgov)

    # Deduplicate by netloc
    df = preprocess(df)

    # ── 2026 statistics ────────────────────────────────────────────────────
    all_sites    = df[df["is_primary_leo"] == True]
    county_eq    = df[df["county_equivalent"] == True]
    top_counties = get_top_counties(df)

    gov_all    = int(all_sites["isgov"].sum())
    den_all    = int(all_sites["isgov"].notna().sum())
    gov_county = int(county_eq["isgov"].sum())
    den_county = int(county_eq["isgov"].notna().sum())
    gov_top    = int(top_counties["isgov"].sum())
    den_top    = int(top_counties["isgov"].notna().sum())

    print("=== 2026 .gov adoption ===")
    print(f"All unique election websites:   {pct(gov_all, den_all)}")
    print(f"County-equivalent websites:     {pct(gov_county, den_county)}")
    print(f"20 most populous counties:      {pct(gov_top, den_top)}")
    print()

    # ── State breakdown ────────────────────────────────────────────────────
    by_state = df[df["is_primary_leo"] == True].groupby("State")["isgov"].agg(
        gov="sum", total="count"
    )
    by_state["pct"] = by_state["gov"] / by_state["total"]
    print("=== .gov adoption by state (top 10) ===")
    print(by_state.sort_values("pct", ascending=False).head(10).to_string())
    print()

    # ── Datawrapper table ──────────────────────────────────────────────────
    # Historical numbers from prior analyses (hardcoded)
    rows = [
        ("All jurisdictions",       1747, 7010, 2138, 6990, gov_all,    den_all),
        ("County-equivalents",       866, 2764, 1131, 2922, gov_county, den_county),
        ("20 most populous counties",  8,   19,   10,   19, gov_top,    den_top),
    ]
    table = pd.DataFrame(rows, columns=[
        "Grouping",
        "2022 .gov", "2022 total",
        "2024 .gov", "2024 total",
        "2026 .gov", "2026 total",
    ])
    for yr in [2022, 2024, 2026]:
        table[f"{yr} %"] = (table[f"{yr} .gov"] / table[f"{yr} total"] * 100).round(1)

    out_path = HERE / "data/2026/dotgov_trend_2026.csv"
    table.to_csv(out_path, index=False)
    print(f"Saved Datawrapper table → {out_path}")
    print(table.to_string(index=False))


if __name__ == "__main__":
    main()
