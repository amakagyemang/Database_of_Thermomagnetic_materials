# Database_of_Thermomagnetic_materials
This is the database of thermomagnetic materials for the MSCA Heat4Energy Project.

Materials Screening
This repository contains Python scripts and the database for automated computational screening of thermomagnetic materials using Density Functional Theory (DFT) and data from materials databases.  
The workflow retrieves data from the Materials Project, Aflowlib, NEMAD, and ICSD, and computes derived properties such as band structures, density of states (DOS), elastic tensors, and magnetic descriptors.



## Structure of Repository
````

---

Downloader/
│
├── fetch\_band\_dos.py        # Fetches band structures + DOS from Materials Project
├── fetch\_elasticity.py      # Fetches elastic tensors and derived properties
├── fetch\_nemad.py           # Queries NEMAD API for magnetic, curie temp, etc.
├── utils\_plot.py            # Utilities for plotting DOS/band JSON into figures
├── datalist.csv             # List of MP IDs (input to fetch scripts)
└── README.md                # This file

````

---

##Dependencies

Install requirements with:

```bash
pip install -r requirements.txt
````

### Python Packages

* `mp-api` — Materials Project API client
* `requests` — for NEMAD/ICSD API calls
* `pandas` — for handling tabular datasets
* `numpy` — numerical utilities
* `matplotlib` — plotting band/DOS results
* `pymatgen` — structure and DFT data parsing
* `tqdm` — progress bars

### External Requirements

* A valid Materials Project API key (set as `MP_API_KEY` environment variable)
* A valid NEMAD API key (set as `NEMAD_API_KEY` environment variable)
* ICSD access via institutional subscription for CIF downloads

---

##  Authentication

Set your API keys as environment variables:

```bash
export MP_API_KEY="your-materialsproject-key"
export NEMAD_API_KEY="your-nemad-key"
```

These are automatically read by the scripts.

---

## Usage

### 1. Fetch Band Structures + DOS

```bash
python3 fetch_band_dos.py --input datalist.csv --output results_band_dos/
```

* Downloads MP band structures (SC, Hinuma, Latimer–Munro paths) and DOS
* Saves JSON data and PNG plots into `results_band_dos/`

### 2. Fetch Elastic Properties

```bash
python3 fetch_elasticity.py --input datalist.csv --output results_elasticity/
```

* Retrieves `elastic_tensor`, bulk/shear moduli, Poisson ratio.
* Outputs CSV summary + JSON files

### 3. Query NEMAD

```bash
python3 fetch_nemad.py --elements "Fe,Co" --db magnetic --limit 20
```

* Queries NEMAD for magnetic materials containing Fe, Co
* Supported DBs: `magnetic`, `magnetic_anisotropy`, `thermoelectric`, `superconductor`
* Outputs JSON/CSV of results

### 4. Plot DOS or Bands from JSON

```bash
python3 utils_plot.py --dos results_band_dos/mp-149_dos.json
python3 utils_plot.py --bands results_band_dos/mp-149_bs.json
```

## Output

* JSON files with raw MP/NEMAD data
* CSV summary tables (elastic constants, band gaps, etc.)
* Publication-ready plots (PNG/PDF) for DOS, bands, elastic summaries


 Workflow
1. Provide `datalist.csv` with a list of MP IDs (one per line).
2. Run `fetch_band_dos.py` and `fetch_elasticity.py` to download electronic and elastic properties.
3. Query NEMAD for experimental metadata.
4. Use `utils_plot.py` to visualize DOS/bands.
5. Combine all results into screening tables (examples provided in the LaTeX paper).

A. Jain *et al.*, **The Materials Project: A materials genome approach to accelerating materials innovation**, *APL Materials* **1**, 011002 (2013).
  NEMAD Database: [https://nemad.org](https://nemad.org)

License: MIT License — free to use, modify, and distribute.

Do you want me to also generate a `requirements.txt` for you with exact pinned versions (e.g. `mp-api==0.41.2`, `pymatgen==2025.x.x`), or keep it unpinned for flexibility?
```
