#!/usr/bin/env python3
import os
import time
import csv
import pandas as pd
from tqdm import tqdm
from mp_api.client import MPRester
from pymatgen.core import Structure  # optional

# --- API key (env preferred) ---
API_KEY = os.getenv("MP_API_KEY") or "xlHLPjD3Tn3sEqZ2SqhHuKGDvCOXqWYI"
mpr = MPRester(API_KEY)

# --- Paths / filenames ---
LOGFILE = "log.txt"
DATADIR = "datadir"
SKIPPED_DIR = "datadir_skipped"
EXCEL_FILE = "datalist.xlsx"
CSV_FILE = "datalist.csv"

all_rows = []

def log_message(message: str):
    print(message)
    with open(LOGFILE, 'a', encoding="utf-8") as f:
        f.write(message + "\n")

def ensure_directories():
    os.makedirs(DATADIR, exist_ok=True)
    os.makedirs(SKIPPED_DIR, exist_ok=True)

def chunk_list(lst, max_chars=55):
    chunks, current_chunk, current_length = [], [], 0
    for item in lst:
        add_len = len(item) + (1 if current_chunk else 0)
        if current_length + add_len > max_chars:
            chunks.append(current_chunk)
            current_chunk = [item]
            current_length = len(item)
        else:
            current_chunk.append(item)
            current_length += add_len
    if current_chunk:
        chunks.append(current_chunk)
    return chunks

def safe_api_call(func, *args, retries=3, delay=5, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log_message(f"Attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    log_message(f"All {retries} attempts failed for {func.__name__} with args {args}")
    return None

def _extract_dbids(doc):
    """
    Tolerate both 'database_IDs' and legacy 'database_Ids' casings.
    Return dict or {}.
    """
    dbids = getattr(doc, "database_IDs", None)
    if not isinstance(dbids, dict):
        dbids = getattr(doc, "database_Ids", None)
    return dbids if isinstance(dbids, dict) else {}

def _extract_icsd_ids(doc) -> str:
    dbids = _extract_dbids(doc)
    icsd = dbids.get("icsd") or []
    try:
        return ";".join(map(str, icsd))
    except Exception:
        return ""

def _extract_dois_from_origins(doc) -> str:
    """
    Summary docs often don't have a top-level 'doi'.
    Pull DOIs from origins[].references[].doi when available.
    """
    dois = set()
    origins = getattr(doc, "origins", None) or []
    try:
        for org in origins:
            refs = getattr(org, "references", None) or []
            for ref in refs:
                # ref may be a dataclass or dict
                d = ref if isinstance(ref, dict) else getattr(ref, "__dict__", {})
                doi = d.get("doi", "") or getattr(ref, "doi", "") or ""
                if isinstance(doi, str) and doi.strip():
                    dois.add(doi.strip())
    except Exception:
        pass
    return ";".join(sorted(dois))

def query_and_save_metadata():
    open(LOGFILE, 'w').close()
    ensure_directories()

    allowed_elements = ["Mn", "Fe", "Co", "Ni", "Cr"]
    banned_elements = [
        "Re","Os","Ir","Pt","Au","In","Tc","Be","As","Cd","Ba","Hg","Tl","Pb","Ac",
        "Cs","Po","Np","U","Pu","Th","He","Ne","Ar","Kr","Xe"
    ]
    banned_chunks = chunk_list(banned_elements)
    all_results = []

    log_message("Searching materials with MP Summary API...")

    # Server-side filter + chunked exclude lists
    for elem in allowed_elements:
        for chunk in banned_chunks:
            results = safe_api_call(
                mpr.materials.summary.search,
                elements=[elem],
                exclude_elements=chunk,
                is_stable=True,
                formation_energy=(None, 0),                         # < 0
                total_magnetization=(0, None),                      # > 0
                total_magnetization_normalized_vol=(0.0386, None),  # > threshold
                num_unique_magnetic_sites=(2, None),                # >= 2
                fields=["material_id"]
            )
            if results:
                all_results.extend(results)

    material_ids = list({doc.material_id for doc in all_results})
    log_message(f"Total unique materials found: {len(material_ids)}")

    columns = [
        "ID","pretty_formula","compound","energy_cell","energy_atom",
        "lattice_system","spacegroup","species","volume_cell",
        "moment_cell","mag_field","mag_sites_MP","mag_sites",
        "mag_type","magmom","comment1","icsd_ids","doi","comment2",
        "e_above_hull","magnetization_norm_vol"
    ]
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        csv.writer(f).writerow(columns)

    batch_size = 500
    for batch_num in range(0, len(material_ids), batch_size):
        batch = material_ids[batch_num:batch_num+batch_size]
        log_message(f"Processing batch {batch_num // batch_size + 1}")

        # IMPORTANT: request fields your mp_api version supports
        basic_data = safe_api_call(
            mpr.materials.summary.search,
            material_ids=batch,
            fields=[
                "material_id","formula_pretty","composition_reduced",
                "volume","elements","symmetry",
                "database_Ids",   # legacy casing (your traceback shows this available)
                "origins"         # use origins to derive DOIs
            ]
        )

        thermo_data = safe_api_call(
            mpr.materials.thermo.search,
            material_ids=batch,
            fields=["material_id","formation_energy_per_atom","energy_above_hull"]
        )

        magnetism_data = safe_api_call(
            mpr.materials.magnetism.search,
            material_ids=batch,
            fields=[
                "material_id","ordering","total_magnetization",
                "num_magnetic_sites","num_unique_magnetic_sites",
                "total_magnetization_normalized_vol","magmoms"
            ]
        )

        if not basic_data:
            log_message(f"Batch {batch_num // batch_size + 1} failed (no basic_data).")
            continue

        thermo_dict = {d.material_id: d for d in (thermo_data or [])}
        magnetism_dict = {d.material_id: d for d in (magnetism_data or [])}

        for doc in tqdm(basic_data, desc=f"Batch {batch_num // batch_size + 1}", unit="material"):
            material_id = doc.material_id
            thermo = thermo_dict.get(material_id)
            magnetism = magnetism_dict.get(material_id)

            # Extra client-side filters (as you had)
            if magnetism is None or magnetism.ordering != "FM":
                continue
            if thermo is None or thermo.formation_energy_per_atom is None or thermo.formation_energy_per_atom >= 0:
                continue

            icsd_ids = _extract_icsd_ids(doc)
            doi = _extract_dois_from_origins(doc)

            row = [
                material_id,
                doc.formula_pretty,
                getattr(doc.composition_reduced, "formula", ""),
                "",  # energy_cell
                thermo.formation_energy_per_atom if thermo else "",
                doc.symmetry.crystal_system if doc.symmetry else "",
                doc.symmetry.symbol if doc.symmetry else "",
                ";".join(str(e) for e in doc.elements),
                doc.volume,
                magnetism.total_magnetization if magnetism else "",
                "",  # mag_field (reserved)
                magnetism.num_magnetic_sites if magnetism else "",
                magnetism.num_unique_magnetic_sites if magnetism else "",
                magnetism.ordering if magnetism else "",
                ";".join(map(str, getattr(magnetism, "magmoms", []))) if hasattr(magnetism, "magmoms") else "",
                "",
                icsd_ids,   # filled via database_Ids/ID(s)
                doi,        # from origins[].references[].doi
                "",
                thermo.energy_above_hull if thermo else "",
                magnetism.total_magnetization_normalized_vol if magnetism else ""
            ]

            all_rows.append(row)
            with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow(row)

    # Save Excel copy
    df = pd.DataFrame(all_rows, columns=columns)
    df.to_excel(EXCEL_FILE, index=False)
    log_message("Metadata saved to datalist.csv and datalist.xlsx")

    # Write ICSD IDs file for your ICSD fetcher
    icsd_flat = []
    for r in all_rows:
        if r[16]:  # icsd_ids column
            icsd_flat.extend([x.strip() for x in str(r[16]).split(";") if x.strip()])
    icsd_flat = sorted({x for x in icsd_flat})
    with open("ids_to_download.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(icsd_flat) + ("\n" if icsd_flat else ""))
    log_message(f"Wrote {len(icsd_flat)} ICSD IDs -> ids_to_download.txt")

def download_structures():
    df = pd.read_csv(CSV_FILE)
    material_ids = df["ID"].tolist()
    log_message(f"Found {len(material_ids)} materials to download structures for.")

    for mid in tqdm(material_ids, desc="Downloading CIFs", unit="material"):
        try:
            s = mpr.materials.structure.get_structure_by_material_id(mid)
            outp = os.path.join(DATADIR, f"{mid}.cif")
            try:
                cif_str = s.to(fmt="cif")
                with open(outp, "w") as fh:
                    fh.write(cif_str)
            except Exception:
                from pymatgen.io.cif import CifWriter
                CifWriter(s).write_file(outp)
        except Exception as e:
            log_message(f"Failed to download {mid}: {e}")

def main():
    query_and_save_metadata()
    download_structures()
    log_message("All steps completed successfully!")

if __name__ == "__main__":
    main()

