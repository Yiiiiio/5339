# COMP5339 Assignment 1 — EV Charger Data Integration

Modules A/B/C have supplied their implementations and handoff CSV files. Module D adds the database build script, notebook, DDL, database design documentation, and quality checks. Recorded database build results are available in `data/final/quality_report.json`.

**This is the Module D integration version, not a final course submission that has passed all submission checks.** C's original API caches and candidate-review files were not included in the original ZIP. The team report and team-wide AI usage report still need to be completed, and another member must independently reproduce the workflow. These outstanding tasks have not been marked as passed.

## 1. Project Structure

| Path | Description |
| --- | --- |
| `notebooks/01 acquire_clean.ipynb` | A: automated acquisition and cleaning (the filename contains a space) |
| `notebooks/02_spatial.ipynb` | B: spatial integration |
| `notebooks/03_augment.ipynb` | C: augmentation using Open Charge Map |
| `notebooks/04_database.ipynb` | D: database construction, validation, queries, and handoff |
| `scripts/build_database.py` | Database build implementation shared with D's notebook |
| `sql/schema.sql` | Standalone DDL for recreating the schema in an empty database |
| `data/raw/ev_20251216.csv` | Official raw dataset from December 2025 |
| `data/raw/SA4_2026_AUST_SHP_GDA2020.zip` | Complete Australian SA4 boundaries and supporting files |
| `data/processed/chargers_clean.csv` | A: 1,958 cleaned source records |
| `data/processed/charger_sa4.csv` | B: 1,958 spatial integration results |
| `data/processed/charger_sa4_review.csv` | B: one unresolved candidate-region entry |
| `data/processed/charger_attributes.csv` | C: 216 accepted augmentation matches |
| `data/final/project.duckdb` | D: final database |
| `data/final/quality_report.json` | Quality metrics, versions, file fingerprints, and known limitations |
| `data/final/sa4_summary.csv` | Source record counts grouped by SA4 |
| `data/final/augmentation_distance_review.csv` | Augmentation matches exceeding 3 km, for review |
| `docs/database_design.md` | Database relationship diagram, design rationale, and spatial details |
| `docs/D_handoff.md` | D's completed work, validation, and team follow-up tasks |
| `docs/AI_usage_D.md` | AI usage record for the team to verify and consolidate |
| `tests/test_database.py` | D's relational constraint and invalid-input protection checks |
| `output/pdf/` | Original assignment instructions and Chinese reference translation |

The Chinese PDF filename in the original ZIP could not be extracted correctly on the local machine. Its original bytes were preserved, and the file was renamed to `COMP5339_2026s2_Assignment1_Chinese.pdf`.

## 2. Environment Setup and Quick Start for D

Open a terminal in the project root. The recorded validation environment used Python 3.11; exact dependency versions are listed in `requirements.txt`.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/build_database.py
```

On Windows, activate the environment with `.venv\Scripts\activate`. The script determines the project root from its own location; an explicit root can also be supplied using `--root PATH`.

Alternatively, launch `python -m jupyterlab`, select this environment's Python kernel, open `notebooks/04_database.ipynb`, restart the kernel, and run all cells. The notebook and command-line script rebuild the same outputs without first rerunning A/B/C.

The first run installs the official DuckDB spatial extension into the project's `.duckdb_extensions/` directory and requires network access. Subsequent runs on the same machine reuse the cache. Extensions depend on the platform and version; machine-specific caches are not included in the ZIP. Use the DuckDB version specified in `requirements.txt` to open the database.

## 3. Rerunning the Full Workflow

1. Open `notebooks/01 acquire_clean.ipynb` in Jupyter and ensure the kernel's working directory is `notebooks/` (A currently uses `Path.cwd().parent`). It downloads the raw CSV and boundaries and generates the cleaned data.
2. Run `02_spatial.ipynb` and `03_augment.ipynb`. B requires `data/raw/sa4_2026/`, extracted by A. D extracts the ZIP into a temporary directory itself and does not depend on that directory.
3. C reads the `OCM_API_KEY` environment variable or reuses `data/external/ocm_pilot_raw.json` and `ocm_australia_raw.json`. The source is the [Open Charge Map API](https://api.openchargemap.io/v3/poi/). See C's notebook for parameters, rules, and review logic. Do not write API keys into files or include them in the submission package.
4. Run D. Whenever inputs change, rerun the affected downstream steps and recheck the reported figures.
5. Have another team member reproduce the workflow in an independent environment.

**Currently missing:** `ocm_pilot_raw.json`, `ocm_australia_raw.json`, `ocm_pilot_candidate_review.csv`, and `ocm_full_candidate_review.csv`. Obtain the corresponding copies used by C to produce this version of the CSV files. Querying the API again retrieves data from a new point in time and must not be presented as the original retrieval snapshot. The full A → B/C → D workflow has not been claimed as reproduced from scratch.

Official raw data sources and programmatic download URLs are retained in A's notebook. File modification times or external verification dates must not be used as substitutes for dataset retrieval dates or augmentation retrieval timestamps.

## 4. Interfaces and Field Conventions

- Exchange data through files and join on `record_id`, never row numbers. A generates `record_id` as a stable hash of the original record content; it does not identify a unique physical site.
- A: `Latitude` and `Longitude` are numeric; `Number_of_plugs` is a positive integer; `power_kw` may be missing. Postcodes, `OBJECTID`, and `record_id` remain text, and quality flags are Boolean. All fields retain their original names in the database.
- `operator_standardized` references the operators table; original operator labels are also retained.
- B: `SA4_CODE26` is text and may be NULL. There is one result per source record. Unmatched records are retained, and candidate regions are used only for review.
- C: the UTF-8 BOM is handled automatically. The source, external ID, and `record_id` form a composite primary key. Connector types are split into a child table, without inferring a one-to-one correspondence between connectors and the list of power values.
- Empty CSV fields become NULL; unknown values are not replaced with zero. New data that violates key relationships, numeric types, or upstream/downstream consistency causes an error, preserving the previous database.
- Point geometries use an assumed EPSG:4326 CRS, while regions use the confirmed EPSG:7844 CRS. Spatial SQL performs explicit transformations, and B's unconfirmed charger CRS status is retained.

## 5. Data Quality and Validation

| Metric | Current handoff data |
| --- | ---: |
| Base source records | 1,958 |
| With SA4 / without SA4 | 1,957 / 1 |
| DC source records / distinct locations at six decimal places | 433 / 430 |
| Augmented DC source records / locations | 216 / 215 |
| Record-level coverage | 49.88% |
| Coverage using C's location definition | 50.00% |
| Augmentation matches exceeding 3 km | 17 |

A match counts as supplying additional attributes when at least one of C's `connector_types`, `external_connection_quantity`, `external_number_of_points`, `external_status`, `external_usage_type`, or `external_usage_cost` fields is non-empty. This follows C's definition; the two coverage measures must be reported separately.

D checks A/B record sets and field consistency, primary and foreign keys, imported row counts, and geometry validity. It also independently checks all of B's results using a DuckDB `within` spatial join. These checks do not establish that source coordinates agree with addresses, nor do they independently reconfirm C's external matches.

Run D's checks:

```bash
python -m unittest discover -s tests -v
```

Example database usage (in the Python environment, from the project root):

```python
from pathlib import Path
from scripts.build_database import connect_database
with connect_database(Path.cwd()) as con:
    print(con.execute("SELECT * FROM sa4_summary WHERE source_records > 0").df())
    print(con.execute("SELECT * FROM augmentation_review").df())
```

For the standalone DDL, create a new DuckDB connection, execute `INSTALL spatial; LOAD spatial;`, and then execute `sql/schema.sql`. The DDL creates the schema only; the build script loads the data. See `docs/database_design.md` for the relationship diagram and design rationale.

## 6. Before Submission

- C must supply the original caches and candidate-review files. The team must review the 17 long-distance matches, reused external IDs, and coverage definitions.
- A/B must confirm the CRS assumption and review the one unmatched record. Do not assign an SA4 code solely from the nearest candidate.
- C should review D's work, and A or another member should reproduce the full workflow in a clean environment.
- The team must complete the project report (up to six pages of main content), member details and contributions, and the team-wide AI usage report. This version provides D's design documentation and AI usage record; it does not provide or claim to be the team's final report.
- The ZIP should contain the code, DDL, database, raw and processed data, augmentation retrieval snapshots, and documentation. Git-ignored files are not automatically included in a repository-export ZIP. A should verify the actual package contents before uploading.
