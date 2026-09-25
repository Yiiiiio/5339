# COMP5339 Assignment 1


## 1. Project Structure

- `notebooks/` contains four notebooks for data acquisition and cleaning, spatial integration, data augmentation, and database construction.
- `scripts/build_database.py` builds and validates the DuckDB database.
- `sql/schema.sql` defines the database tables, constraints, and views.
- `data/raw/` stores the original charger CSV and SA4 boundary ZIP.
- `data/processed/` stores the cleaned data, SA4 assignments, spatial review records, and augmentation results.
- `data/external/` stores the external API responses and matching review files. 
- `data/final/` contains the final DuckDB database.
- `requirements.txt` lists the required Python packages.
- `README.md` explains the project structure, environment setup, execution steps, assumptions, and output files.
  
## 2. Setup Instructions

Install Python 3.11 before following the steps below.
Open a terminal in the project root folder.

On macOS or Linux:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

On Windows (Command Prompt):

```bat
py -3.11 -m venv .venv
.venv\Scripts\activate
```

Then install the dependencies and start Jupyter:

```bash
python -m pip install -r requirements.txt
python -m ipykernel install --user --name comp5339 --display-name "Python (COMP5339)"
jupyter lab
```

Select the `Python (COMP5339)` kernel when opening the notebooks.

## 3. Execution Instructions

Start JupyterLab from the project root. Open the notebooks in `notebooks/` and select the `Python (COMP5339)` kernel. Run all cells in each notebook in the following order:

1. `01_acquire_clean.ipynb`: Downloads and cleans the charger data, downloads and extracts the SA4 boundaries, and saves `data/processed/chargers_clean.csv`.
2. `02_spatial.ipynb`: Assigns chargers to SA4 regions and saves `charger_sa4.csv` and `charger_sa4_review.csv` in `data/processed/`.
3. `03_augment.ipynb`: Uses Open Charge Map data to add charger attributes and saves `data/processed/charger_attributes.csv`. It uses local API caches when available; otherwise, an API key is required. New API data may change the results.
4. `04_database.ipynb`: Builds `data/final/project.duckdb` and runs validation and example queries.

Keep the original folder structure. The first notebook expects its working directory to be `notebooks/`. Rerunning the notebooks overwrites generated files.

### Database Rebuild

To rebuild only the database, keep the four CSV files in `data/processed/` and the SA4 ZIP in `data/raw/`. Run `04_database.ipynb`, or run this command from the project root:

```bash
python scripts/build_database.py
```

An Open Charge Map API key is not required for this step.

### Validation

Run the tests from the project root:

```bash
python -m unittest discover -s tests -v
```

With the supplied data, the database should contain 1,958 charger records and 216 augmentation records, with 1,957 confirmed SA4 assignments and one unmatched record.

## 4. Assumptions Made

- Each `record_id` represents one source record, not necessarily one physical charging site. Records sharing coordinates or addresses are retained and flagged for review.
- Charger coordinates are assumed to use EPSG:4326. They are transformed to match the SA4 boundaries in EPSG:7844 before spatial matching. Unmatched records remain unassigned.
- Missing values are not treated as zero. Postcodes extracted from addresses are marked as inferred, while conflicting postcode values remain unresolved.
- Only single power values with an explicit kW unit are converted to `power_kw`. Other formats are preserved separately. `Upcoming` is treated as a status, not a charger type.
- Operator aliases are merged only where supporting evidence is available. Ambiguous names are retained for review.
- Augmentation covers DC records. For coverage calculation, locations are grouped by coordinates rounded to six decimal places. This grouping does not guarantee that each location represents a separate physical site.
- External matches use location and other available attributes. Accepted matches may still contain errors, especially where source coordinates are inaccurate.

## 5. Outputs and Included Data Files

### Raw Data

- `data/raw/ev_20251216.csv`: The original December 2025 NSW charger dataset.
- `data/raw/SA4_2026_AUST_SHP_GDA2020.zip`: The original 2026 ABS SA4 boundary archive.

### Processed Data

- `data/processed/chargers_clean.csv`: Contains 1,958 cleaned charger records, standardized fields, and quality review flags.
- `data/processed/charger_sa4.csv`: Contains SA4 assignments for all charger records. Unmatched records have no assigned region.
- `data/processed/charger_sa4_review.csv`: Contains candidate regions for unresolved spatial matches. These are not confirmed assignments.
- `data/processed/charger_attributes.csv`: Contains 216 accepted augmentation records from Open Charge Map, including additional attributes and matching information.
  
### External Data Files

The following files are included in `data/external/`:

- `ocm_pilot_raw.json`: Cached Open Charge Map responses for the first 30 DC records.
- `ocm_australia_raw.json`: A cached Australian Open Charge Map dataset containing 1,367 records.
- `ocm_pilot_candidate_review.csv`: Candidate matches and review decisions from the pilot stage.
- `ocm_full_candidate_review.csv`: Candidate matches, matching scores, and review decisions for the full augmentation stage.

### Database

- `data/final/project.duckdb`: The final DuckDB database containing charger records, operators, SA4 regions, spatial assignments, augmentation attributes, and validation metadata.
- `sql/schema.sql`: Defines the database tables, constraints, and views.