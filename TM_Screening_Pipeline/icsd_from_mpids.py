#!/usr/bin/env python3
"""
icsd_from_mpids.py
Read mp-ids from datalist.csv, fetch ICSD cross-refs via mp_api, and write
ids_to_download.txt (one ICSD ID per line).

- Auto-detects 'database_IDs' vs 'database_Ids' on Summary
- Optional provenance fallback for IDs missing from Summary
- Adds --debug to see how many ICSDs are found per batch

Usage:
  export MP_API_KEY=your_key
  python icsd_from_mpids.py
  # or thorough:
  python icsd_from_mpids.py --use-provenance
"""

import os, sys, argparse, time
from pathlib import Path
from typing import List, Set, Any, Dict

import pandas as pd
from tqdm import tqdm

try:
    from mp_api.client import MPRester
    from mp_api.client.core.client import MPRestError
except Exception:
    print("ERROR: mp_api is not installed. Run: pip install mp_api", file=sys.stderr)
    sys.exit(1)


def read_mpids(csv_path: Path) -> List[str]:
    df = pd.read_csv(csv_path)
    if "ID" not in df.columns:
        raise ValueError(f"'ID' column not found in {csv_path}")
    ids = [str(x).strip() for x in df["ID"].dropna().astype(str) if str(x).strip()]
    if not ids:
        raise ValueError("No MP IDs found in datalist.csv")
    return sorted(set(ids))


def looks_like_icsd(s: str) -> bool:
    return s.replace(".", "").isdigit()


def extract_icsd_from_dbids(dbids_like: Any) -> List[str]:
    if isinstance(dbids_like, dict):
        icsd = dbids_like.get("icsd") or []
        if isinstance(icsd, (list, tuple)):
            return [str(x) for x in icsd if str(x).strip()]
    return []


def detect_dbids_field(mpr: MPRester, mpids: List[str]) -> str:
    """Probe the Summary API to see which casing is supported."""
    try:
        _ = mpr.materials.summary.search(material_ids=mpids[:3], fields=["material_id", "database_IDs"])
        return "database_IDs"
    except MPRestError:
        pass
    # try the alternate casing
    _ = mpr.materials.summary.search(material_ids=mpids[:3], fields=["material_id", "database_Ids"])
    return "database_Ids"


def fetch_from_summary(mpr: MPRester, mpids: List[str], batch: int, retries: int, dbids_field: str, debug: bool=False) -> Set[str]:
    icsd: Set[str] = set()
    for i in range(0, len(mpids), batch):
        chunk = mpids[i:i + batch]
        # we let mp_api handle pagination internally; _search is used behind the scenes
        for attempt in range(1, retries + 1):
            try:
                docs = mpr.materials.summary.search(material_ids=chunk, fields=["material_id", dbids_field])
                break
            except MPRestError as e:
                if attempt >= retries:
                    raise
                time.sleep(1.5 * attempt)
        count_before = len(icsd)
        for d in docs or []:
            dbids = getattr(d, dbids_field, None)
            icsd.update(extract_icsd_from_dbids(dbids))
        if debug:
            got = len(icsd) - count_before
            print(f"[DEBUG] Summary batch {i//batch+1}: +{got} ICSD IDs (total {len(icsd)})")
    return icsd


def fetch_from_provenance(mpr: MPRester, mpids: List[str], retries: int, debug: bool=False) -> Set[str]:
    out: Set[str] = set()
    for idx, mid in enumerate(tqdm(mpids, desc="Provenance", unit="mat")):
        for attempt in range(1, retries + 1):
            try:
                prov = mpr.provenance.get_data_by_id(mid, fields=["database_IDs", "database_Ids"])
                dbids = getattr(prov, "database_IDs", None)
                if not isinstance(dbids, dict):
                    dbids = getattr(prov, "database_Ids", None)
                out.update(extract_icsd_from_dbids(dbids))
                break
            except Exception:
                if attempt >= retries:
                    # continue to the next mp-id
                    break
                time.sleep(1.5 * attempt)
        if debug and (idx + 1) % 100 == 0:
            print(f"[DEBUG] Provenance scanned {idx+1} / {len(mpids)} (ICSD so far: {len(out)})")
    return out


def main():
    ap = argparse.ArgumentParser(description="Extract ICSD IDs (via mp_api) from mp-ids listed in datalist.csv")
    ap.add_argument("--csv", default="datalist.csv", help="Input CSV with 'ID' column")
    ap.add_argument("--out", default="ids_to_download.txt", help="Output file for ICSD IDs")
    ap.add_argument("--batch", type=int, default=250, help="Batch size for summary API")
    ap.add_argument("--retries", type=int, default=3, help="Retries per batch/request")
    ap.add_argument("--use-provenance", action="store_true", help="Also query provenance per MP ID (slower)")
    ap.add_argument("--debug", action="store_true", help="Print per-batch counts")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    out_path = Path(args.out)

    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found", file=sys.stderr)
        sys.exit(1)

    mpids = read_mpids(csv_path)
    print(f"[INFO] Loaded {len(mpids)} MP IDs from {csv_path}")

    api_key = os.getenv("MP_API_KEY")
    if not api_key:
        print("WARNING: MP_API_KEY not set; mp_api may still use ~/.mp_api_key", file=sys.stderr)

    with MPRester(api_key=api_key) as mpr:
        # Detect field casing once
        dbids_field = detect_dbids_field(mpr, mpids)
        if args.debug:
            print(f"[DEBUG] Using Summary field: {dbids_field}")

        # 1) Fast, batched Summary harvesting
        icsd = fetch_from_summary(mpr, mpids, batch=args.batch, retries=args.retries, dbids_field=dbids_field, debug=args.debug)
        print(f"[INFO] ICSD IDs from summary: {len(icsd)}")

        # 2) Optional provenance enrichment
        if args.use_provenance:
            addl = fetch_from_provenance(mpr, mpids, retries=args.retries, debug=args.debug)
            before = len(icsd)
            icsd |= addl
            print(f"[INFO] Added {len(icsd) - before} via provenance (total {len(icsd)})")

    # Keep only numeric-like IDs; write file
    icsd_list = sorted({s for s in (x.strip() for x in icsd) if s and looks_like_icsd(s)})
    out_path.write_text("\n".join(icsd_list) + ("\n" if icsd_list else ""), encoding="utf-8")
    print(f"[OK] Wrote {len(icsd_list)} ICSD IDs -> {out_path}")


if __name__ == "__main__":
    main()

