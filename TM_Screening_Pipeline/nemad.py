#!/usr/bin/env python3
"""
nemad_fetch.py

Query the NEMAD API using formulas and/or element sets derived from your
MP 'datalist.csv' and save standardized results for downstream merging.

Features
- Uses exact formula endpoint (/api/<type>/formula) when you pass --by-formula
- Else uses element search (/api/<type>/search) from each formula's element set
- Supports database types: magnetic, magnetic_anisotropy, thermoelectric, superconductor
- Auto handles exact_match, batching, retries, and simple rate limiting
- Outputs tidy CSV/JSON; optional merged CSV with MP rows

Auth
- Provide your API key via env var NEMAD_API_KEY or --api-key
- API base: https://api.nemad.org

Examples
  export NEMAD_API_KEY=YOUR_KEY
  # Magnetic by exact formula (best precision):
  python nemad_fetch.py --types magnetic --by-formula

  # Magnetic + anisotropy by element search, allow supersets, 500/ms backoff:
  python nemad_fetch.py --types magnetic magnetic_anisotropy --exact-match false --sleep-ms 500

  # Save JSON, Parquet, and a merged CSV with MP columns included:
  python nemad_fetch.py --types magnetic --by-formula --json --parquet --merge-out nemad_merged_with_mp.csv
"""

from __future__ import annotations
import os, sys, time, json, argparse
from pathlib import Path
from typing import Dict, List, Tuple, Any

import pandas as pd

try:
    import requests
except Exception:
    print("Please `pip install requests pandas`", file=sys.stderr)
    sys.exit(1)

# Optional: better formula handling (recommended)
try:
    from pymatgen.core.composition import Composition
except Exception:
    Composition = None

BASE_URL = "https://api.nemad.org"
VALID_TYPES = {"magnetic", "magnetic_anisotropy", "thermoelectric", "superconductor"}


def read_mp_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "ID" not in df.columns:
        raise ValueError("MP CSV must have an 'ID' column.")
    # choose a composition column
    comp_col = "compound" if "compound" in df.columns else (
        "pretty_formula" if "pretty_formula" in df.columns else None
    )
    if comp_col is None:
        raise ValueError("MP CSV needs a 'compound' or 'pretty_formula' column.")
    return df.rename(columns={comp_col: "_mp_formula"})


def canonical_formula(s: str) -> str:
    if not isinstance(s, str) or not s.strip():
        return ""
    if Composition is None:
        # fallback: return the string as-is
        return s.strip()
    try:
        return Composition(s).reduced_formula
    except Exception:
        return s.strip()


def elements_from_formula(s: str) -> List[str]:
    if Composition is None:
        # naive fallback: split on capitals
        import re
        return sorted(set(re.findall(r"[A-Z][a-z]?", s)))
    try:
        comp = Composition(s)
        return sorted(comp.get_el_amt_dict().keys())
    except Exception:
        return []


def request_json(
    url: str, headers: Dict[str, str], params: Dict[str, Any], retries: int = 3, backoff: float = 0.8
) -> Dict[str, Any] | None:
    for attempt in range(1, retries + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception:
                return None
        # simple backoff
        if attempt < retries:
            time.sleep(backoff * attempt)
    # final failure
    sys.stderr.write(f"[WARN] {url} failed with {resp.status_code}: {resp.text[:200]}\n")
    return None


def fetch_by_formula(db_type: str, formulas: List[str], headers: Dict[str, str], limit: int, sleep_ms: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for f in formulas:
        if not f:
            continue
        data = request_json(
            f"{BASE_URL}/api/{db_type}/formula",
            headers=headers,
            params={"formula": f, "limit": limit},
        )
        time.sleep(max(0, sleep_ms) / 1000.0)
        if not data or "results" not in data:
            continue
        for rec in data["results"]:
            rec["_query_formula"] = f
            rec["_db_type"] = db_type
            out.append(rec)
    return out


def fetch_by_elements(
    db_type: str, formulas: List[str], headers: Dict[str, str], exact_match: bool, limit: int, sleep_ms: int
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for f in formulas:
        els = elements_from_formula(f)
        if not els:
            continue
        params = {
            "elements": ",".join(els),
            "exact_match": str(bool(exact_match)).lower(),
            "limit": limit,
        }
        data = request_json(f"{BASE_URL}/api/{db_type}/search", headers=headers, params=params)
        time.sleep(max(0, sleep_ms) / 1000.0)
        if not data or "results" not in data:
            continue
        for rec in data["results"]:
            rec["_query_elements"] = ",".join(els)
            rec["_query_formula_hint"] = f
            rec["_db_type"] = db_type
            out.append(rec)
    return out


def flatten_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    # Flatten and normalize keys; keep order-ish
    df = pd.json_normalize(records, sep=".")
    # common NEMAD fields seen in examples
    preferred_cols = [
        "_db_type",
        "Material_Name",
        "Curie", "Neel",
        "Crystal_Structure",
        "Magnetic_Moment",
        "DOI",
        "_query_formula", "_query_elements", "_query_formula_hint",
    ]
    # move preferred columns to the front if present
    front = [c for c in preferred_cols if c in df.columns]
    rest = [c for c in df.columns if c not in front]
    return df[front + rest]


def main():
    ap = argparse.ArgumentParser(description="Fetch NEMAD data and write tidy CSV/JSON; optionally merge with MP CSV.")
    ap.add_argument("--mp", default="datalist.csv", help="Path to MP classic CSV (default: datalist.csv)")
    ap.add_argument("--types", nargs="+", default=["magnetic"], help="NEMAD DB types: magnetic, magnetic_anisotropy, thermoelectric, superconductor")
    ap.add_argument("--by-formula", action="store_true", help="Use formula endpoint instead of element search")
    ap.add_argument("--exact-match", default="true", choices=["true", "false"], help="For element search: exact element set match")
    ap.add_argument("--limit", type=int, default=50, help="Max results per query (-1 for all if supported)")
    ap.add_argument("--sleep-ms", type=int, default=250, help="Delay between API calls (ms) to be nice to the server")
    ap.add_argument("--retries", type=int, default=3, help="Retries per call")
    ap.add_argument("--api-key", default=os.getenv("NEMAD_API_KEY", ""), help="NEMAD API key (or set NEMAD_API_KEY env var)")
    ap.add_argument("--out", default="nemad_results.csv", help="CSV of raw NEMAD results")
    ap.add_argument("--json", action="store_true", help="Also write JSON dump (nemad_results.json)")
    ap.add_argument("--parquet", action="store_true", help="Also write Parquet dump (nemad_results.parquet)")
    ap.add_argument("--merge-out", default="", help="If set, also write merged MP+NEMAD CSV to this path")
    args = ap.parse_args()

    # Validate db types
    types = []
    for t in args.types:
        t = t.strip().lower()
        if t not in VALID_TYPES:
            raise SystemExit(f"Unknown db type '{t}'. Valid: {', '.join(sorted(VALID_TYPES))}")
        types.append(t)

    if not args.api_key:
        print("WARNING: NEMAD API key not set. Provide with --api-key or env NEMAD_API_KEY.", file=sys.stderr)

    headers = {
        "X-API-Key": args.api_key,
        "accept": "application/json",
    }

    # 1) Load MP CSV & compute canonical formula once
    mp_path = Path(args.mp)
    mp = read_mp_csv(mp_path)
    mp["_formula_canon"] = mp["_mp_formula"].astype(str).map(canonical_formula)
    formulas = sorted(set(mp["_formula_canon"].tolist()))

    # 2) Query NEMAD per formula (or element set)
    all_records: List[Dict[str, Any]] = []
    for db_type in types:
        if args.by_formula:
            recs = fetch_by_formula(db_type, formulas, headers, limit=args.limit, sleep_ms=args.sleep_ms)
        else:
            recs = fetch_by_elements(
                db_type,
                formulas,
                headers,
                exact_match=(args.exact_match.lower() == "true"),
                limit=args.limit,
                sleep_ms=args.sleep_ms,
            )
        all_records.extend(recs)

    # 3) Flatten + write results
    df_nemad = flatten_records(all_records)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df_nemad.to_csv(args.out, index=False)
    print(f"[OK] Wrote NEMAD results -> {args.out}  (rows={len(df_nemad)})")

    if args.json:
        Path("nemad_results.json").write_text(json.dumps(all_records), encoding="utf-8")
        print("[OK] Wrote nemad_results.json")

    if args.parquet:
        try:
            df_nemad.to_parquet("nemad_results.parquet", index=False)
            print("[OK] Wrote nemad_results.parquet")
        except Exception as e:
            print(f"[WARN] parquet not written: {e}")

    # 4) Optional merge onto MP rows (by canonical formula)
    if args.merge_out:
        if df_nemad.empty:
            print("[INFO] Skipping merge (no NEMAD rows).")
        else:
            # add canonical formula to NEMAD side: prefer explicit Material_Name, else the query hint
            nemad_formula_col = "Material_Name" if "Material_Name" in df_nemad.columns else (
                "_query_formula" if "_query_formula" in df_nemad.columns else "_query_formula_hint"
            )
            df_nemad["_formula_canon"] = df_nemad[nemad_formula_col].astype(str).map(canonical_formula)
            merged = mp.merge(df_nemad, on="_formula_canon", how="left", suffixes=("_mp", "_nemad"))
            merged.to_csv(args.merge_out, index=False)
            print(f"[OK] Wrote merged MP+NEMAD -> {args.merge_out}  (rows={len(merged)})")


if __name__ == "__main__":
    main()

