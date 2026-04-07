# Engine Failure Analysis — Medallion Architecture

## Project Overview

This project implements a **Medallion Architecture** (Bronze → Silver → Gold) pipeline for engine failure analysis, using Python (pandas). The solution is designed to run in **Databricks** as a single notebook, or locally with plain Python.

### Layers

| Layer | Records | Columns | Description |
|-------|---------|---------|-------------|
| **Bronze** | 319 | 15 | Raw ingest — all records and columns, no changes |
| **Silver** | 314 | 15 | Only full, valid records; expanded column names; correct types |
| **Gold** | 313 | 12 | Semantic layer; only informative columns; passed all quality tests |

---

## Files

```
engine_medallion.py    ← Main pipeline script (run this)
dashboard.html         ← Interactive data quality dashboard (open in browser)
source_data.csv        ← Input data (place in the same folder)
bronze_engine.csv      ← Generated: Bronze layer output
silver_engine.csv      ← Generated: Silver layer output
gold_engine.csv        ← Generated: Gold layer output
rejected_records.csv   ← Generated: Records that failed quality checks
README.md              ← This file
```

---

## How to Run

### Option A — Locally (Python 3.8+)

**1. Install dependencies**
```bash
pip install pandas
```

**2. Run the pipeline**
```bash
# Make sure source_data.csv is in the same directory
python engine_medallion.py
```

**3. View the dashboard**
```
Open dashboard.html in any browser (Chrome, Firefox, Edge, Safari).
No server required — it is a static HTML file.
```

---

### Option B — Databricks (Recommended)

**1. Set up a free account**
- Go to [https://www.databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition)
- Create a free Community Edition account

**2. Upload the source file**
- In Databricks, go to **Data → Add Data → Upload File**
- Upload `source_data.csv`
- Note the DBFS path, e.g. `/FileStore/tables/source_data.csv`

**3. Create a new notebook**
- Go to **Workspace → Create → Notebook**
- Language: **Python**
- Cluster: start or create a cluster

**4. Import the script**
- Either paste the contents of `engine_medallion.py` into cells, or
- Upload the `.py` file and use `%run ./engine_medallion`

**5. Update the source path**
At the top of the script, change:
```python
SOURCE_FILE = "source_data.csv"
```
to the DBFS path:
```python
SOURCE_FILE = "/dbfs/FileStore/tables/source_data.csv"
```

**6. Run All Cells**
- Click **Run All** (Ctrl+Shift+Enter)
- Output tables will be saved as CSV files in the working directory

**7. Dashboard (Extra)**
- In Databricks, use the **Dashboard** feature in the notebook toolbar
- Alternatively, open `dashboard.html` in your local browser — it requires no server

---

## Quality Tests

### Input Tests (applied at Silver)

| Test | What is checked | Example failures |
|------|----------------|------------------|
| Test 1 — Missing values | Any non-always-null column is NaN or empty | Rows 168, 254 |
| Test 2 — Type mismatches | Numeric columns contain non-numeric strings | `oph="kkkkk"`, `rpm_max="kartsi"` |
| Test 3 — Invalid domain values | Values outside allowed set per column | `issue_type="ddddd"`, `pist_m=2` |

**Valid domain values (from business description):**
- `issue_type`: `typical`, `atypical`, `non-related`, `non-symptomatic`
- `resting_analysis_results`: `0`, `1`, `2`
- `pist_m`, `past_dmg`, `full_load_issues`, `high_breakdown_risk`: `0` or `1`

### Business Test (applied at Gold)

| Test | Rule |
|------|------|
| OPH limit | `operating_hours <= 120,000` |

---

## Column Reference (Silver / Gold)

| Gold Column | Bronze Column | Type | Description |
|-------------|---------------|------|-------------|
| `operating_hours` | `oph` | Int | Engine operating hours |
| `piston_material` | `pist_m` | Int (0/1) | Piston material type |
| `combustion_issue_type` | `issue_type` | String | Combustion issue classification |
| `brake_mean_effective_pressure` | `bmep` | Int | Average pressure forcing pistons |
| `natural_gas_impurities` | `ng_imp` | Int | Gas impurities in nmol |
| `past_damage` | `past_dmg` | Int (0/1) | Engine had past damage |
| `resting_analysis_results` | `resting_analysis_results` | Int (0/1/2) | Post-operation resting results |
| `rpm_maximum` | `rpm_max` | Int | Max rotations per minute |
| `full_load_issues` | `full_load_issues` | Int (0/1) | Issues induced by full load |
| `number_of_unplanned_events` | `number_up` | Int | Count of unplanned events |
| `number_of_turbo_chargers` | `number_tc` | Int | Number of turbo chargers installed |
| `high_breakdown_risk` | `high_breakdown_risk` | Int (0/1) | Target: breakdown risk |

**Dropped at Gold** (non-informative):
- `operational_setting_1` — constant value (always 1)
- `operational_setting_2` — 100% null in source
- `operational_setting_3` — constant value (always 0)

---

## Key Findings

- **319 total records** ingested at Bronze
- **5 records rejected** at Silver due to input quality failures (1.57%)
- **1 record excluded** at Gold due to business rule (OPH = 1,000,000,000)
- **3 columns dropped** at Gold (no informational value)
- In the Gold dataset: 54.3% of engines are flagged as **high breakdown risk**
- Most common issue type: **typical** (47.3%), followed by **non-related** (28.8%)
- Average operating hours: **54,214 hours**
