"""
recrawl_2026.py — idempotent, concurrent re-crawl for the 2026 .gov analysis.

For each office we build an ordered list of candidate start URLs and follow
redirects, recording whether the office's site resolves to .gov. ".gov wins":
we stop at the first candidate that resolves to .gov, so an office is never
falsely downgraded just because one stale address stopped redirecting.

Candidate order per office:
  1. May-2026 final destination, if it was already .gov   (lock in a confirmed .gov)
  2. Fresh March-2026 CTCL listed URL                     (catches new/changed sites)
  3. May-2026 final destination, if valid (non-.gov)      (where we landed last cycle)
  4. Original CTCL-listed URL                             (last resort)
Each seed is expanded to scheme/www variants only as needed; candidates are
deduped and capped.

Idempotent & resumable (keyed by Office UUID):
  - outcome in {gov, nongov, dead} is FINAL  -> skipped on re-run
  - outcome == transient (timeout/conn/SSL/403/5xx, no success) -> RETRIED on re-run
This is what makes a low per-request timeout safe: anything slow-but-alive just
gets another attempt next run instead of being miscounted. Run repeatedly to mop
up non-responders; use --finalize-transient for a last generous pass that demotes
any remaining transient rows to dead.

Output: data/2026/recrawl_2026.csv  (does NOT touch the May LEO_combined_redirects_2026.csv)
Provenance sidecar: data/2026/recrawl_2026_provenance.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd
import requests
import urllib3
from requests.exceptions import (RequestException, SSLError, Timeout,
                                  TooManyRedirects, ConnectionError as ReqConnErr)
from tldextract import extract as tld
from tqdm import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HERE = Path(__file__).resolve().parent
BASE_MAY = HERE / "data/2026/LEO_combined_redirects_2026.csv"          # May crawl (base + cols)
FRESH_CTCL = HERE.parent / "ctcl/data/from_ctcl_202603/EXTERNAL LEOD_combined_2025.csv"
OUTPUT = HERE / "data/2026/recrawl_2026.csv"
PROVENANCE = HERE / "data/2026/recrawl_2026_provenance.json"

SAVE_INTERVAL = 200
DEFAULT_WORKERS = 24
DEFAULT_TIMEOUT = 5          # READ timeout (s); low on purpose — re-runs catch transient failures
CONNECT_TIMEOUT = 3          # connect timeout (s); dead hosts fail fast here
FINALIZE_TIMEOUT = 15        # generous last-pass read timeout for --finalize-transient
MAX_REDIRECTS = 10           # bound redirect chains; default 30 can stall for minutes
MAX_ERROR_ATTEMPTS = 3       # stop trying candidates after this many errored -> transient
MAX_CANDIDATES = 6
MAX_ROOT_TRIES = 4           # bare-domain-root fallbacks to try when a path 404s
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

ERROR_SENTINELS = {"all endpoints failed"}
FINAL_OUTCOMES = {"gov", "nongov", "dead"}


def is_gov_url(u: str | None) -> bool:
    if not isinstance(u, str) or not u:
        return False
    if u.startswith("http"):
        return tld(u).suffix == "gov"
    return u.rstrip("/").endswith(".gov")


def valid_url(u) -> bool:
    """True if u looks like a usable URL/host (not NaN, not an error sentinel)."""
    if not isinstance(u, str):
        return False
    s = u.strip()
    if not s:
        return False
    if s.lower() in ERROR_SENTINELS or s.lower().startswith("error:"):
        return False
    return True


def with_scheme(u: str) -> str:
    u = u.strip()
    return u if u.startswith("http") else f"https://{u}"


def expand(url: str) -> list[str]:
    """A seed URL plus scheme/www variants, in a sensible try order."""
    url = with_scheme(url)
    parts = urlsplit(url)
    scheme, netloc, path = parts.scheme, parts.netloc, parts.path or ""
    host = netloc[4:] if netloc.startswith("www.") else netloc
    other = "http" if scheme == "https" else "https"
    variants = [
        url,
        f"{scheme}://{host}{path}",
        f"{scheme}://www.{host}{path}",
        f"{other}://{host}{path}",
        f"{other}://www.{host}{path}",
    ]
    return list(dict.fromkeys(variants))


def bare_roots(url: str) -> list[str]:
    """The scheme://host/ roots (with and without www) for a URL — used as a
    fallback when a listed deep path 404s but the site has moved to .gov at the root."""
    parts = urlsplit(with_scheme(url))
    host = parts.netloc[4:] if parts.netloc.startswith("www.") else parts.netloc
    if not host:
        return []
    return [f"https://{host}/", f"https://www.{host}/"]


def build_candidates(row: pd.Series) -> list[str]:
    """Ordered, deduped candidate URLs for one office (see module docstring)."""
    may = row.get("website_redirect")
    fresh = row.get("fresh_url")
    orig = row.get("website")

    seeds: list[str] = []
    if valid_url(may) and is_gov_url(may):
        seeds.append(may)                 # 1. confirmed .gov first
    if valid_url(fresh):
        seeds.append(fresh)               # 2. fresh CTCL URL
    if valid_url(may):
        seeds.append(may)                 # 3. last cycle's destination
    if valid_url(orig):
        seeds.append(orig)                # 4. original

    candidates: list[str] = []
    for s in seeds:
        for v in expand(s):
            if v not in candidates:
                candidates.append(v)
            if len(candidates) >= MAX_CANDIDATES:
                return candidates
    return candidates


def fetch(url: str, session: requests.Session, read_timeout: int):
    """Return (final_url, status_code, err_kind). err_kind is None on HTTP response."""
    try:
        r = session.get(url, allow_redirects=True, timeout=(CONNECT_TIMEOUT, read_timeout),
                        headers=HEADERS, verify=False)
        return r.url, r.status_code, None
    except Timeout:
        return None, None, "timeout"
    except TooManyRedirects:
        return None, None, "redirects"
    except SSLError:
        return None, None, "ssl"
    except ReqConnErr as e:
        # DNS failures surface here; treat name-resolution as definitive-dead
        kind = "dns" if "NameResolutionError" in str(e) or "Name or service" in str(e) \
            or "getaddrinfo" in str(e) else "conn"
        return None, None, kind
    except RequestException:
        return None, None, "exc"


# error kinds that should be RETRIED on a later run vs. treated as definitively dead
RETRYABLE = {"timeout", "ssl", "conn", "exc", "redirects"}
DEAD_KINDS = {"dns"}


def crawl_office(row: pd.Series, timeout: int) -> dict:
    candidates = build_candidates(row)
    session = requests.Session()
    session.max_redirects = MAX_REDIRECTS
    first_nongov_url = None
    first_nongov_code = None
    roots: list[str] = []          # bare roots to try when a path 404s (possible site move)
    err_kinds: set[str] = set()
    error_attempts = 0

    for url in candidates:
        final_url, code, err = fetch(url, session, timeout)
        if err is not None:
            err_kinds.add(err)
            error_attempts += 1
            if error_attempts >= MAX_ERROR_ATTEMPTS:
                break   # office is very likely unreachable; re-run will retry transient
            continue
        # Classification is by the final URL's TLD regardless of status code — a 404
        # page served from a .gov is still .gov — matching the original methodology.
        if is_gov_url(final_url):
            return _result("gov", final_url, code, candidates)   # .gov wins -> stop
        if first_nongov_url is None:
            first_nongov_url, first_nongov_code = final_url, code
        if code != 200:
            # the listed page is gone/blocked; the site may have moved to .gov at the
            # root (e.g. acvote.org/index 404s but acvote.org -> *.gov). Queue the root.
            roots.extend(bare_roots(final_url or url))

    # Bare-root fallback: only UPGRADES to .gov, never downgrades a live non-.gov page.
    tried: set[str] = set()
    for root in roots:
        if root in tried:
            continue
        tried.add(root)
        if len(tried) > MAX_ROOT_TRIES:
            break
        final_url, code, err = fetch(root, session, timeout)
        if err is None and is_gov_url(final_url):
            res = _result("gov", final_url, code, candidates)
            res["via_root"] = True
            return res

    if first_nongov_url is not None:
        return _result("nongov", first_nongov_url, first_nongov_code, candidates)

    # Carry-forward: an office confirmed .gov in the prior cycle is never regressed
    # by a mere failure to reach it now — no evidence is not evidence of leaving .gov.
    may = row.get("website_redirect")
    if valid_url(may) and is_gov_url(may):
        res = _result("gov", may, None, candidates)
        res["carried_forward"] = True
        return res

    # No candidate returned any HTTP response — connection-level failure only.
    only_dns = bool(err_kinds) and err_kinds.issubset(DEAD_KINDS)
    outcome = "dead" if only_dns else "transient"
    return _result(outcome, None, None, candidates)


def _result(outcome, final_url, code, candidates) -> dict:
    return {
        "outcome": outcome,
        "recrawl_final_url": final_url,
        "recrawl_status_code": code,
        "recrawl_isgov": outcome == "gov",
        "n_candidates": len(candidates),
        "carried_forward": False,
        "via_root": False,
        "last_crawled": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def load_base() -> pd.DataFrame:
    base = pd.read_csv(BASE_MAY, low_memory=False)
    fresh = pd.read_csv(FRESH_CTCL, low_memory=False)[
        ["Office UUID (maps to govproj)", "Website"]
    ].rename(columns={"Office UUID (maps to govproj)": "Office UUID", "Website": "fresh_url"})
    fresh = fresh.dropna(subset=["fresh_url"]).drop_duplicates(subset=["Office UUID"], keep="first")
    return base.merge(fresh, on="Office UUID", how="left")


RESULT_COLS = ["outcome", "recrawl_final_url", "recrawl_status_code",
               "recrawl_isgov", "n_candidates", "carried_forward", "via_root", "last_crawled"]


def reconcile_existing() -> None:
    """Apply the deterministic carry-forward rule to an existing output file
    without re-crawling: any dead/transient office that was confirmed .gov in the
    prior cycle is restored to .gov."""
    df = pd.read_csv(OUTPUT, low_memory=False)
    if "carried_forward" not in df.columns:
        df["carried_forward"] = False
    mask = df["outcome"].isin(["dead", "transient"]) & df["website_redirect"].apply(
        lambda u: valid_url(u) and is_gov_url(u))
    n = int(mask.sum())
    df.loc[mask, "outcome"] = "gov"
    df.loc[mask, "recrawl_final_url"] = df.loc[mask, "website_redirect"]
    df.loc[mask, "recrawl_isgov"] = True
    df.loc[mask, "carried_forward"] = True
    atomic_save(df)
    counts = df["outcome"].value_counts(dropna=False).to_dict()
    print(f"[reconcile] carried forward {n} confirmed-.gov offices (dead/transient -> gov)")
    print(f"[reconcile] outcome counts: { {str(k): int(v) for k,v in counts.items()} }")


def atomic_save(df: pd.DataFrame) -> None:
    tmp = OUTPUT.with_suffix(".csv.tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, OUTPUT)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--limit", type=int, default=None, help="smoke-test: crawl at most N offices")
    ap.add_argument("--finalize-transient", action="store_true",
                    help="generous last pass; demote still-failing transient rows to dead")
    ap.add_argument("--fresh", action="store_true",
                    help="ignore any existing output and re-crawl every office from scratch")
    ap.add_argument("--reconcile-only", action="store_true",
                    help="apply the carry-forward rule to the existing output; no crawling")
    args = ap.parse_args()

    if args.reconcile_only:
        reconcile_existing()
        return

    df = load_base()

    # restore prior results if resuming (unless --fresh)
    if OUTPUT.exists() and not args.fresh:
        prev = pd.read_csv(OUTPUT, low_memory=False)
        prev_cols = [c for c in RESULT_COLS if c in prev.columns]
        df = df.merge(prev[["Office UUID", *prev_cols]], on="Office UUID", how="left")
    else:
        for c in RESULT_COLS:
            df[c] = pd.NA

    # only crawl offices with at least one usable candidate
    has_candidate = df.apply(lambda r: bool(build_candidates(r)), axis=1)

    done_mask = df["outcome"].isin(FINAL_OUTCOMES)
    if args.finalize_transient:
        # last pass: retry transient with a generous timeout
        todo_mask = (df["outcome"] == "transient") & has_candidate
        timeout = FINALIZE_TIMEOUT
    else:
        todo_mask = (~done_mask) & has_candidate
        timeout = args.timeout

    pending_idx = list(df.index[todo_mask])
    if args.limit:
        pending_idx = pending_idx[: args.limit]

    n_final = int(done_mask.sum())
    print(f"[recrawl] total offices: {len(df)} | already final: {n_final} | "
          f"to crawl this run: {len(pending_idx)} | timeout={timeout}s | workers={args.workers}",
          file=sys.stderr)
    if not pending_idx:
        print("[recrawl] nothing to do.", file=sys.stderr)
        _write_provenance(df)
        return

    completed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(crawl_office, df.loc[i], timeout): i for i in pending_idx}
        for fut in tqdm(as_completed(futures), total=len(futures)):
            i = futures[fut]
            try:
                res = fut.result()
            except Exception as e:  # never let one office kill the run
                res = _result("transient", None, None, [])
                res["error"] = f"{type(e).__name__}: {e}"
            for k, v in res.items():
                if k in df.columns:
                    df.at[i, k] = v
            completed += 1
            if completed % SAVE_INTERVAL == 0:
                atomic_save(df)

    if args.finalize_transient:
        still = df["outcome"] == "transient"
        df.loc[still, "outcome"] = "dead"
        print(f"[recrawl] finalize: demoted {int(still.sum())} transient -> dead", file=sys.stderr)

    atomic_save(df)
    _write_provenance(df)

    counts = df["outcome"].value_counts(dropna=False).to_dict()
    print(f"[recrawl] done. outcome counts: {counts}", file=sys.stderr)


def _write_provenance(df: pd.DataFrame) -> None:
    prov = {
        "recrawl_completed_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "base_may_crawl": str(BASE_MAY),
        "fresh_ctcl_export": str(FRESH_CTCL),
        "dotgov_registry": "data/2026/dotgov_registry_current_full.csv "
                           "(cisagov/dotgov-data commit 849504b, 2026-06-29)",
        "outcome_counts": {str(k): int(v) for k, v in
                           df["outcome"].value_counts(dropna=False).items()},
    }
    PROVENANCE.write_text(json.dumps(prov, indent=2))


if __name__ == "__main__":
    main()
