# Election Office Websites — Project Context

This repo supports two related but separate projects: a BPC analysis of .gov adoption by local election offices, and a link-checking service provided to CTCL for their Local Election Office Dataset (LEOD).

```
dotgov/     BPC .gov adoption analysis (public)
ctcl/       CTCL link check scripts (public); ctcl/data/ is gitignored
blog/       published and draft blog posts
archive/    superseded files (old notebook)
```

---

## BPC .gov Adoption Analysis

### What it does
Tracks the share of local election office websites using the verified `.gov` domain over time, producing a three-year comparison chart and summary statistics.

### Data files (`dotgov/data/` — gitignored)

```
dotgov/data/
├── reference/
│   └── county_adjacency2023.txt         Static Census file mapping county names to states.
│                                        Used every cycle to identify county-equivalent jurisdictions.
├── 2024/
│   ├── LEO_combined_2024.csv            Input dataset for the 2024 and 2026 analyses. Built from
│                                        CTCL's LEOD. Contains is_primary_leo — do not replace with
│                                        a newer CTCL export, as this flag is not recoverable from
│                                        Office responsibilities alone.
│   ├── LEO_combined_redirects_2024.csv  2024 crawl output: same rows as above with website_redirect
│                                        and website_status appended.
│   ├── comparison_2022_2024.csv/.xlsx   Intermediate analysis output from the 2024 cycle.
└── 2026/
    ├── LEO_combined_redirects_2026.csv  2026 crawl output: output of fetch_redirects.py.
    └── dotgov_trend_2026.csv            Datawrapper import table: numerators, denominators, and
                                         percentages for all three groupings across 2022/2024/2026.
```

### Scripts
- `dotgov/fetch_redirects.py` — re-crawls every URL in `LEO_combined_2024.csv` fresh (no caching — we can't know if a redirect changed without visiting), follows redirects, records the final URL. Resumable: saves every 200 rows, skips already-completed UUIDs on restart. Output: `2026/LEO_combined_redirects_2026.csv`.
- `dotgov/analyze.py` — reads the redirects file, merges county-equivalent flags from `reference/county_adjacency2023.txt`, computes `isgov` from the final URL's TLD, deduplicates by netloc (preferring `is_primary_leo=True` rows), prints statistics, and saves `2026/dotgov_trend_2026.csv` for import into Datawrapper.

### Key methodology decisions
- **Unit of analysis**: unique netlocs among `is_primary_leo == True` rows. This matches CDT's 2022 methodology and allows year-over-year comparison.
- **Deduplication**: where multiple offices share a final domain, keep the row where `is_primary_leo=True` (then `county_equivalent=True`). This is why `preprocess()` sorts ascending on those flags and uses `keep='last'`.
- **County-equivalents**: identified by joining against `dotgov/data/county_adjacency2023.txt`. The "county-equivalents" grouping in the plot is the subset of deduplicated rows where `county_equivalent=True`.
- **Top 20 counties**: hardcoded list by 2020 Census population (`TOP_20` in `analyze.py`). NYC is a special case — all five boroughs are served by one Board of Elections row whose `County` field contains all five borough names.
- **No SSL verification**: `verify=False` throughout. Many election sites have certificate issues that don't affect real-world accessibility.
- **Historical numbers**: 2022 figures are from CDT's analysis; 2024 figures are from BPC's July 2024 analysis. Both are hardcoded in `analyze.py`.

### 2026 results (as of May 2026)
| Grouping | 2022 | 2024 | 2026 |
|---|---|---|---|
| All jurisdictions | 25% (1747/7010) | 31% (2138/6990) | 37% (2499/6699) |
| County-equivalents | 31% (866/2764) | 39% (1131/2922) | 47% (1300/2798) |
| 20 most populous counties | 42% (8/19) | 53% (10/19) | 58% (11/19) |

Notable changes since 2024: San Bernardino County CA and Wayne County MI adopted .gov; Santa Clara County CA moved from `vote.santaclaracounty.gov` to `sccgov.org` (the .gov subdomain still exists but CTCL updated the listed URL).

---

## CTCL Link Check

### What it does
Checks every website and Facebook link in CTCL's LEOD for broken/dead links and returns the annotated dataset to CTCL for their annual refresh.

### Data lineage (`ctcl/data/` — gitignored)
- `ctcl/data/from_ctcl_202603/EXTERNAL LEOD_combined_2025.csv` — raw file received from CTCL (March 2026). Do not modify.
- `ctcl/data/CTCL_2025_link_check.csv` — same rows with result columns appended. This is the deliverable to CTCL.

### Scripts (run in order)
1. `ctcl/check_ctcl_links.py` — initial pass: checks `Website` (live/redirected/http_error/dead) and `FBID` (live) using `requests`. Resumable by UUID. Output: `CTCL_2025_link_check.csv`.
2. `ctcl/recheck_ctcl_links.py` — re-checks `website_status` in [dead, http_error] and `fb_status == dead` with `requests`; updates only if result improves. Updates CSV in place.
3. `ctcl/recheck_ctcl_links_browser.py` — re-checks remaining `http_error` rows using headless Playwright/Chromium, which bypasses most bot-blocking 403s. Updates only if result improves.
4. `ctcl/check_ctcl_facebook_browser.py` — visits all FBID rows using an **authenticated** Playwright session (you log in manually at the start), extracts the page name from the DOM (second `<h1>`, skipping "Notifications"), and fuzzy-matches it against the office name. Run with `--limit N` for testing.

### Result columns added to CTCL_2025_link_check.csv
- `website_final_url` — final URL after redirects
- `website_status_code` — HTTP status code
- `website_status` — `live` / `redirected` / `http_error` / `dead`
- `fb_outcome` — `live` / `not_found`
- `fb_page_title` — Facebook page name extracted from DOM
- `fb_name_match` — fuzzy match score (0–100) vs. office name
- `fb_name_flag` — True if score < 60 or outcome != live

### May 2026 results
| | Count |
|---|---|
| Website: live | 7,484 |
| Website: redirected | 612 |
| Website: http_error | 1,041 |
| Website: dead | 254 |
| Website: no website listed | 2,707 |
| Facebook: live | 1,421 |
| Facebook: not_found | 149 |
| Facebook: live but name-flagged | 15 |

### Notes
- The column `Office UUID (maps to govproj)` was renamed to `Office UUID` during processing; it was renamed back before delivery.
- Facebook's tab title includes notification counts (e.g. `(1) Facebook`) and is not usable — extract the page name from the DOM instead.
- `http_error` rows that persist after the browser recheck are likely genuinely broken (though a small number may be aggressive WAFs).
- The 15 name-flagged Facebook rows are mostly personal pages for individual officeholders. One outlier: Doddridge County Commission WV links to "Michelle Holly - Fayette County" (wrong county).

### Python environment
Use `/Users/will/.pyenv/versions/3.10.13/bin/python3`. The default `python3` shim does not have the required packages. Key packages: `pandas`, `requests`, `tldextract`, `playwright`, `rapidfuzz`, `tqdm`.
