#!/usr/bin/env python3
"""
fetch_band_dos.py

Read mp-ids from datalist.csv (column 'ID') and download:
  - Electronic band structures (line-mode): Setyawan–Curtarolo (SC), Hinuma, Latimer–Munro
  - Electronic band structure (uniform k-mesh)  [--uniform]
  - Electronic DOS
  - Phonon DOS                                  [--phonon]
  - Phonon band structure (line-mode)           [--phonon-bs if supported by your client]

Files are saved as JSON (pymatgen objects' .as_dict()) to:
  <out_dir>/<mpid>/
    bs_sc.json
    bs_hinuma.json
    bs_latimer_munro.json
    bs_uniform.json
    dos.json
    ph_dos.json
    ph_bs.json

Usage:
  export MP_API_KEY=your_key
  python fetch_band_dos.py
  # options:
  python fetch_band_dos.py --uniform --phonon
  python fetch_band_dos.py --uniform --phonon --phonon-bs
"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import List

import pandas as pd
from tqdm import tqdm

# mp_api client
from mp_api.client import MPRester
# band path styles
from emmet.core.electronic_structure import BSPathType


def read_mpids(csv_path: Path) -> List[str]:
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found")
    df = pd.read_csv(csv_path)
    if "ID" not in df.columns:
        raise ValueError(f"'ID' column not found in {csv_path}")
    ids = [str(x).strip() for x in df["ID"].dropna().astype(str) if str(x).strip()]
    if not ids:
        raise ValueError("No MP IDs found in datalist.csv")
    return sorted(set(ids))


def save_json(obj, path: Path):
    """Serialize a pymatgen object (or dict-like) to JSON."""
    try:
        data = obj.as_dict() if hasattr(obj, "as_dict") else obj
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f)
        return True
    except Exception as e:
        print(f"[WARN] Could not save {path.name}: {e}")
        return False


def fetch_electronic_data(mpr: MPRester, mpid: str, out_dir: Path, want_uniform: bool):
    """Fetch electronic band structures (SC/Hinuma/Latimer–Munro [+uniform]) and DOS."""
    sub = out_dir / mpid
    sub.mkdir(parents=True, exist_ok=True)

    # Band structures (line-mode)
    try:
        bs_sc = mpr.get_bandstructure_by_material_id(mpid, path_type=BSPathType.setyawan_curtarolo)
        save_json(bs_sc, sub / "bs_sc.json")
    except Exception as e:
        print(f"[WARN] {mpid}: SC bandstructure failed: {e}")

    try:
        bs_hin = mpr.get_bandstructure_by_material_id(mpid, path_type=BSPathType.hinuma)
        save_json(bs_hin, sub / "bs_hinuma.json")
    except Exception as e:
        print(f"[WARN] {mpid}: Hinuma bandstructure failed: {e}")

    try:
        bs_lm = mpr.get_bandstructure_by_material_id(mpid, path_type=BSPathType.latimer_munro)
        save_json(bs_lm, sub / "bs_latimer_munro.json")
    except Exception as e:
        print(f"[WARN] {mpid}: Latimer–Munro bandstructure failed: {e}")

    # Uniform band structure (k-mesh)
    if want_uniform:
        try:
            bs_u = mpr.get_bandstructure_by_material_id(mpid, line_mode=False)
            save_json(bs_u, sub / "bs_uniform.json")
        except Exception as e:
            print(f"[WARN] {mpid}: uniform bandstructure failed: {e}")

    # DOS
    try:
        dos = mpr.get_dos_by_material_id(mpid)
        save_json(dos, sub / "dos.json")
    except Exception as e:
        print(f"[WARN] {mpid}: DOS failed: {e}")


def fetch_phonon_data(mpr: MPRester, mpid: str, out_dir: Path, want_ph_bs: bool):
    """Fetch phonon DOS and (if available) phonon band structure."""
    sub = out_dir / mpid
    sub.mkdir(parents=True, exist_ok=True)

    # Phonon DOS
    try:
        ph_dos = mpr.get_phonon_dos_by_material_id(mpid)
        save_json(ph_dos, sub / "ph_dos.json")
    except Exception as e:
        print(f"[WARN] {mpid}: phonon DOS failed: {e}")

    # Phonon band structure (not in all clients; attempt and warn if missing)
    if want_ph_bs:
        try:
            ph_bs = mpr.get_phonon_bandstructure_by_material_id(mpid)
            save_json(ph_bs, sub / "ph_bs.json")
        except Exception as e:
            print(f"[WARN] {mpid}: phonon bandstructure failed (maybe unsupported): {e}")


def main():
    ap = argparse.ArgumentParser(
        description="Fetch electronic/phonon band structures and DOS for MP IDs in datalist.csv"
    )
    ap.add_argument("--csv", default="datalist.csv", help="Input CSV with 'ID' column (default: datalist.csv)")
    ap.add_argument("--out", default="es_data", help="Output directory (default: es_data)")
    ap.add_argument("--batch", type=int, default=100, help="Local batch size per API session (default: 100)")
    ap.add_argument("--uniform", action="store_true", help="Also fetch uniform (k-mesh) band structures")
    ap.add_argument("--phonon", action="store_true", help="Also fetch phonon DOS")
    ap.add_argument("--phonon-bs", action="store_true", help="Also fetch phonon band structures (if supported)")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    out_dir = Path(args.out)

    if not os.getenv("MP_API_KEY"):
        print("WARNING: MP_API_KEY not set; mp_api may still use ~/.mp_api_key", file=sys.stderr)

    mpids = read_mpids(csv_path)
    print(f"[INFO] Loaded {len(mpids)} MP IDs from {csv_path}")

    # Simple local batching (open one API context per chunk)
    for i in range(0, len(mpids), args.batch):
        chunk = mpids[i:i + args.batch]
        print(f"[INFO] Processing {len(chunk)} materials (chunk {i//args.batch + 1})")
        with MPRester() as mpr:
            for mid in tqdm(chunk, unit="mat"):
                fetch_electronic_data(mpr, mid, out_dir, want_uniform=args.uniform)
                if args.phonon or args.phonon_bs:
                    fetch_phonon_data(mpr, mid, out_dir, want_ph_bs=args.phonon_bs)

    print(f"[DONE] Data written under: {out_dir}")


if __name__ == "__main__":
    main()

