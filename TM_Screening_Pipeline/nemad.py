from __future__ import annotations
import os, sys, re, time, json, argparse
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd

# --- deps ---
try:
    import requests
except Exception:
    print("Please `pip install requests pandas`", file=sys.stderr)
    sys.exit(1)

try:
    from pymatgen.core.composition import Composition
except Exception:
    Composition = None


BASE_URL = "https://api.nemad.org"
VALID_TYPES = {"magnetic", "magnetic_anisotropy", "thermoelectric", "superconductor"}


def read_mp_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # formula column (prefer reduced/pretty if present to ensure there are no error limitations)
    comp_col = None
    for cand in ("compound", "pretty_formula", "formula_pretty", "Composition", "composition"):
        if cand in df.columns:
            comp_col = cand
            break
    if comp_col is None:
        raise ValueError("datalist.csv needs a formula column: one of 'compound', 'pretty_formula', 'formula_pretty'.")

    sg_col = None
    for cand in ("spacegroup", "space_group", "space_group_symbol", "SpaceGroup", "spg_symbol"):
        if cand in df.columns:
            sg_col = cand
            break
    if sg_col is None:
        raise ValueError("datalist.csv needs a space-group symbol column (e.g. 'spacegroup').")

    # keep only what we need...
    keep = ["ID", comp_col, sg_col]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()
    df = df.rename(columns={comp_col: "_mp_formula", sg_col: "_mp_sg"})
    return df


def canonical_formula(s: str) -> str:
    if not isinstance(s, str) or not s.strip():
        return ""
    if Composition is None:
        # crude fallback: collapse whitespace
        return re.sub(r"\s+", "", s.strip())
    try:
        return Composition(s).reduced_formula
    except Exception:
        return re.sub(r"\s+", "", s.strip())


_SG_SUBS = {
    "\u2212": "-",  # minus
    "\u2011": "-",  # non-breaking hyphen
    "\u2012": "-",  # figure dash
    "\u2013": "-",  # en dash
    "\u2014": "-",  # em dash
    "\u2010": "-",  # hyphen
    "\u00AF": "-",  # macron sometimes used in copy/paste
    " ": "",
    "_": "",
}

def _normalize_hyphens(s: str) -> str:
    for k, v in _SG_SUBS.items():
        s = s.replace(k, v)
    return s

def canonical_spacegroup_symbol(s: str) -> str:
    """Canonicalize SG symbol (string). Examples:
       'Fm-3m', 'Fm-3m', 'FM-3M', ' f m - 3 m ' -> 'FM-3M'
    """
    if not isinstance(s, str):
        return ""
    s = s.strip()
    if not s:
        return ""
    s = _normalize_hyphens(s)
    # Collapse multiple hyphens
    s = re.sub(r"-{2,}", "-", s)
    # Remove junk around parentheses
    s = re.sub(r"\(\s*\d+\s*\)", "", s)  # drop trailing (225) if present
    # Uppercase letters, keep digits and '-' and '/'
    # Remove stray dots that sometimes appear
    s = re.sub(r"[.\s]", "", s).upper()
    return s

def extract_sg_symbol_from_nemad(rec: Dict[str, Any]) -> str:
    """
    Try several common fields. If a field contains 'Fm-3m (225)' or similar,
    take the symbol-like prefix.
    """
    candidates = []
    for key in ("Space_Group", "SpaceGroup", "space_group", "Crystal_Structure", "crystal_structure"):
        if key in rec and isinstance(rec[key], str) and rec[key].strip():
            candidates.append(rec[key].strip())

    # If nothing obvious, try to scan all string values for something SG-like
    if not candidates:
        for v in rec.values():
            if isinstance(v, str) and any(ch in v for ch in ("m", "3", "-")):
                candidates.append(v)

    for raw in candidates:
        # take the token before a parenthesis or after colon
        token = re.split(r"[\(\):,;]| sg | space ?group ", raw, flags=re.I)[0].strip()
        # if too short, fallback to raw
        token = token if len(token) >= 2 else raw
        canon = canonical_spacegroup_symbol(token)
        if canon:
            return canon
    return ""


def request_json(
    url: str, headers: Dict[str, str], params: Dict[str, Any], retries: int = 3, backoff: float = 0.8
) -> Dict[str, Any] | None:
    err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            err = str(e)
        if attempt < retries:
            time.sleep(backoff * attempt)
    sys.stderr.write(f"[WARN] {url} failed: {err}\n")
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
            rec["_db_type"] = db_type
            rec["_query_formula"] = f
            out.append(rec)
    return out


def flatten_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.json_normalize(records, sep=".")
    # put useful columns first if present
    preferred = [
        "_db_type",
        "Material_Name",
        "DOI",
        "Curie", "Neel",
        "Magnetic_Moment",
        "Crystal_Structure",
        "Space_Group", "SpaceGroup", "space_group",
        "_query_formula",
    ]
    front = [c for c in preferred if c in df.columns]
    rest = [c for c in df.columns if c not in front]
    return df[front + rest]


def main():
    ap = argparse.ArgumentParser(description="Match NEMAD by formula+spacegroup from datalist.csv, save CSV and per-record JSON.")
    ap.add_argument("--mp", default="datalist.csv", help="Path to MP CSV (default: datalist.csv)")
    ap.add_argument("--types", nargs="+", default=["magnetic"], help=f"NEMAD db types (default: magnetic). Options: {', '.join(sorted(VALID_TYPES))}")
    ap.add_argument("--limit", type=int, default=50, help="Max results per NEMAD query")
    ap.add_argument("--sleep-ms", type=int, default=250, help="Delay between API calls (ms)")
    ap.add_argument("--retries", type=int, default=3, help="Retries per API call")
    ap.add_argument("--api-key", default=os.getenv("NEMAD_API_KEY", ""), help="NEMAD API key (or env NEMAD_API_KEY)")
    ap.add_argument("--out", default="nemad_matches.csv", help="CSV of matched (formula+SG) NEMAD results")
    ap.add_argument("--download-dir", default="nemad_raw", help="Directory to save per-record JSON")
    args = ap.parse_args()

    db_types = []
    for t in args.types:
        t = t.strip().lower()
        if t not in VALID_TYPES:
            raise SystemExit(f"Unknown db type '{t}'. Valid: {', '.join(sorted(VALID_TYPES))}")
        db_types.append(t)

    if not args.api_key:
        print("WARNING: NEMAD API key not set. Provide with --api-key or env NEMAD_API_KEY.", file=sys.stderr)

    headers = {
        "X-API-Key": args.api_key,
        "accept": "application/json",
    }

    # load MP CSV amd compute canonical fields
    mp = read_mp_csv(Path(args.mp))
    mp["_formula_canon"] = mp["_mp_formula"].astype(str).map(canonical_formula)
    mp["_sg_canon"] = mp["_mp_sg"].astype(str).map(canonical_spacegroup_symbol)

    # filter out rows without needed info
    mp = mp[(mp["_formula_canon"] != "") & (mp["_sg_canon"] != "")]
    if mp.empty:
        raise SystemExit("No rows with both a valid formula and space-group symbol found in datalist.csv.")

    formulas = sorted(set(mp["_formula_canon"].tolist()))

    # query NEMAD by formula (In any form)
    all_records: List[Dict[str, Any]] = []
    for db_type in db_types:
        recs = fetch_by_formula(db_type, formulas, headers, limit=args.limit, sleep_ms=args.sleep_ms)
        all_records.extend(recs)

    if not all_records:
        print("[INFO] No NEMAD results returned for the given formulas.")
        pd.DataFrame(columns=["_db_type"]).to_csv(args.out, index=False)
        return

    # also we by the Space-group ....also, keep only those whose SG matches the one from MP for the same formula
    # biuld MP lookup, formula, set of allowed SGs
    mp_allowed = (mp.groupby("_formula_canon")["_sg_canon"]
                    .apply(lambda s: set(s.dropna().tolist()))
                    .to_dict())

    matched: List[Dict[str, Any]] = []
    for rec in all_records:
        f = rec.get("_query_formula", "")
        sg_rec = extract_sg_symbol_from_nemad(rec)
        rec["_nemad_sg_canon"] = sg_rec
        # keep if SG matches any MP SG for that formula
        if f and sg_rec and sg_rec in mp_allowed.get(f, set()):
            matched.append(rec)

    # write outputs
    df_out = flatten_records(matched)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(args.out, index=False)
    print(f"[OK] Wrote matched (formula+SG) results -> {args.out}  (rows={len(df_out)})")

    # download raw JSON per record
    outdir = Path(args.download_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    def safe_name(s: str) -> str:
        return re.sub(r"[^A-Za-z0-9._+-]+", "_", s)[:200]

    for i, rec in enumerate(matched, start=1):
        f = rec.get("_query_formula", "NA")
        sg = rec.get("_nemad_sg_canon", "NA")
        dbt = rec.get("_db_type", "NA")
        fname = safe_name(f"{i:05d}_{dbt}_{f}_{sg}.json")
        (outdir / fname).write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] Saved {len(matched)} JSON records in {outdir}")


if __name__ == "__main__":
    main()

