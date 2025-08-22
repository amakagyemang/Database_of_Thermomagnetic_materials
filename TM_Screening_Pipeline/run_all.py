#!/usr/bin/env python3
"""
run_all.py — one-button pipeline to run your whole flow.

Steps:
  1) Materials Project runner (your mp.py) -> datalist.csv, datadir/*
  2) Extract ICSD IDs (icsd_from_mpids.py) -> ids_to_download.txt
  3) (optional) Fetch ICSD CIFs (ICSD_get_cifs_by_ids.py)
  4) Fetch elasticity (fetch_elasticity.py) -> elasticity_summary.csv (+ JSON if set)
  5) Fetch electronic BS/DOS (fetch_band_dos.py) -> es_data/<mpid>/*.json
  6) Fetch NEMAD (nemad_fetch.py) -> nemad_results.csv (+ merged file if set)
  7) (optional) Plot DOS jsons to PNGs (plots under es_plots/)

Configure via CLI flags or env vars. Skips steps if inputs missing or tools unavailable.
"""

from __future__ import annotations
import os, sys, shutil, subprocess, argparse
from pathlib import Path

# ---------- helpers ----------

def run(cmd: list[str], cwd: Path | None = None) -> None:
    print(f"\n$ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)

def exists(p: str | Path) -> bool:
    return Path(p).exists()

def ensure_exe_py(script: str):
    if not exists(script):
        raise SystemExit(f"Required script not found: {script}. Put it in this folder.")

def warn(msg: str):
    print(f"[WARN] {msg}")

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description="Run the full MP → ICSD → Elastic → ES → NEMAD pipeline")
    ap.add_argument("--skip-mp", action="store_true", help="Skip mp.py (assume datalist.csv already exists)")
    ap.add_argument("--skip-icsd", action="store_true", help="Skip fetching ICSD CIFs after creating ids_to_download.txt")
    ap.add_argument("--skip-elastic", action="store_true", help="Skip fetching elasticity")
    ap.add_argument("--skip-es", action="store_true", help="Skip fetching electronic band/DOS")
    ap.add_argument("--skip-nemad", action="store_true", help="Skip fetching NEMAD")
    ap.add_argument("--plot-dos", action="store_true", help="Plot all DOS JSONs under es_data/*/dos.json to es_plots/")
    ap.add_argument("--nemad-types", nargs="+", default=["magnetic"], help="NEMAD DBs: magnetic, magnetic_anisotropy, thermoelectric, superconductor")
    ap.add_argument("--nemad-by-formula", action="store_true", help="Query NEMAD by exact formula endpoint instead of element search")
    ap.add_argument("--nemad-merge-out", default="nemad_merged_with_mp.csv", help="Merged MP+NEMAD CSV path ('' to disable)")
    ap.add_argument("--es-uniform", action="store_true", help="Also fetch uniform (k-mesh) band structures")
    ap.add_argument("--es-phonon", action="store_true", help="Also fetch phonon DOS")
    ap.add_argument("--es-phonon-bs", action="store_true", help="Also fetch phonon band structures (if supported by client)")
    ap.add_argument("--elastic-json-dir", default="elasticity_json", help="Folder to dump per-material elasticity JSON ('' to skip)")
    ap.add_argument("--icsd-with-provenance", action="store_true", help="Make ICSD list using provenance fallback (if your summary lacks links)")
    args = ap.parse_args()

    here = Path.cwd()

    # Check the helper scripts you already have / I provided
    ensure_exe_py("mp.py")
    ensure_exe_py("icsd_from_mpids.py")  # or icsd.py if you named it that
    ensure_exe_py("fetch_elasticity.py")
    ensure_exe_py("fetch_band_dos.py")
    ensure_exe_py("nemad_fetch.py")
    if not exists("ICSD_get_cifs_by_ids.py") and not args.skip_icsd:
        warn("ICSD_get_cifs_by_ids.py not found; skipping ICSD CIF download.")
        args.skip_icsd = True

    # Step 1 — MP run
    if not args.skip_mp:
        # Your mp.py is a "single-shot" runner that does IDs+CSV+CIFs
        run([sys.executable, "mp.py"])
        if not exists("datalist.csv"):
            raise SystemExit("datalist.csv not produced by mp.py")
    else:
        if not exists("datalist.csv"):
            raise SystemExit("--skip-mp set but datalist.csv not found")

    # Step 2 — ICSD extraction
    icsd_cmd = [sys.executable, "icsd_from_mpids.py"]
    if args.icsd_with_provenance:
        icsd_cmd.append("--use-provenance")
    run(icsd_cmd)
    if exists("ids_to_download.txt") and not args.skip_icsd and exists("ICSD_get_cifs_by_ids.py"):
        # ICSD downloader expects ids_to_download.txt in CWD
        try:
            run([sys.executable, "ICSD_get_cifs_by_ids.py"])
        except subprocess.CalledProcessError:
            warn("ICSD_get_cifs_by_ids.py failed; continuing.")

    # Step 3 — Elasticity
    if not args.skip_elastic:
        elastic_args = [sys.executable, "fetch_elasticity.py"]
        if args.elastic_json_dir:
            elastic_args += ["--json-dir", args.elastic_json_dir]
        run(elastic_args)
        if not exists("elasticity_summary.csv"):
            warn("elasticity_summary.csv missing after fetch; continuing.")

    # Step 4 — Electronic structures (bands/DOS)
    if not args.skip_es:
        es_args = [sys.executable, "fetch_band_dos.py"]
        if args.es_uniform:
            es_args.append("--uniform")
        if args.es_phonon:
            es_args.append("--phonon")
        if args.es_phonon_bs:
            es_args.append("--phonon-bs")
        run(es_args)
        if not exists("es_data"):
            warn("es_data/ not created; skipping DOS plots.")
            args.plot_dos = False

    # Step 5 — NEMAD fetch + optional merge
    if not args.skip_nemad:
        nemad_args = [sys.executable, "nemad_fetch.py", "--types", *args.nemad_types]
        if args.nemad_by_formula:
            nemad_args.append("--by-formula")
        if args.nemad_merge_out:
            nemad_args += ["--merge-out", args.nemad_merge_out]
        run(nemad_args)

    # Step 6 — Optional: plot every DOS JSON to PNG
    if args.plot_dos and exists("es_data"):
        plotter = """
import json, matplotlib.pyplot as plt
from pathlib import Path

root = Path("es_data")
out_root = Path("es_plots"); out_root.mkdir(exist_ok=True)
count = 0
for mpdir in root.glob("*/"):
    dos_json = mpdir / "dos.json"
    if not dos_json.exists(): continue
    try:
        data = json.loads(dos_json.read_text())
        energies = data.get("energies", [])
        dens = data.get("densities", {})
        plt.figure(figsize=(6,4))
        if "spin_up" in dens:
            plt.plot(energies, dens["spin_up"], label="Spin ↑")
        if "spin_down" in dens:
            plt.plot(energies, [-x for x in dens["spin_down"]], label="Spin ↓")
        if "total" in dens:
            plt.plot(energies, dens["total"], label="Total")
        plt.axvline(0.0, linestyle="--", linewidth=1)
        plt.xlabel("Energy (eV)"); plt.ylabel("DOS (states/eV)")
        plt.title(mpdir.name)
        plt.legend(); plt.tight_layout()
        out_png = out_root / f"{mpdir.name}_dos.png"
        plt.savefig(out_png, dpi=160); plt.close()
        count += 1
    except Exception as e:
        pass
print(f"[OK] Wrote {count} DOS plots -> {out_root}/")
"""
        run([sys.executable, "-c", plotter])

    print("\n[ALL DONE] ✅")

if __name__ == "__main__":
    main()
