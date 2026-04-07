# Databricks notebook source
# MAGIC %md
# MAGIC # Engine Failure Analysis — Medallion Architecture
# MAGIC
# MAGIC This notebook implements a Bronze → Silver → Gold pipeline for engine failure analysis.
# MAGIC
# MAGIC **Layers:**
# MAGIC - **Bronze**: Raw ingest — all records and columns
# MAGIC - **Silver**: Valid records only; expanded column names; correct types  
# MAGIC - **Gold**: Semantic layer; informative columns; passed all quality + business tests
# MAGIC
# MAGIC **Before running:** Upload `source_data.csv` to DBFS and update `SOURCE_FILE` path below.

# COMMAND ----------

# =============================================================================
# 0.  CONFIGURATION
# =============================================================================
SOURCE_FILE         = "workspace.default.source_data"  # <-- update if needed
OPH_BUSINESS_LIMIT  = 120_000

VALID_ISSUE_TYPES   = {"typical", "atypical", "non-related", "non-symptomatic"}
VALID_RESTING       = {0, 1, 2}
VALID_BOOL_VALS     = {0, 1}
VALID_PIST_M        = {0, 1}

NUMERIC_COLS_RAW = [
    "oph", "pist_m", "bmep", "ng_imp", "past_dmg",
    "resting_analysis_results", "rpm_max", "full_load_issues",
    "number_up", "number_tc", "op_set_1", "op_set_3", "high_breakdown_risk",
]

COLUMN_RENAME = {
    "oph":                      "operating_hours",
    "pist_m":                   "piston_material",
    "issue_type":               "combustion_issue_type",
    "bmep":                     "brake_mean_effective_pressure",
    "ng_imp":                   "natural_gas_impurities",
    "past_dmg":                 "past_damage",
    "resting_analysis_results": "resting_analysis_results",
    "rpm_max":                  "rpm_maximum",
    "full_load_issues":         "full_load_issues",
    "number_up":                "number_of_unplanned_events",
    "number_tc":                "number_of_turbo_chargers",
    "op_set_1":                 "operational_setting_1",
    "op_set_2":                 "operational_setting_2",
    "op_set_3":                 "operational_setting_3",
    "high_breakdown_risk":      "high_breakdown_risk",
}

import pandas as pd
print("Configuration loaded.")

# COMMAND ----------

# MAGIC %md ## 1. Bronze Layer — Raw Ingest

# COMMAND ----------

# DBTITLE 1,Cell 4
df_bronze = spark.table(SOURCE_FILE).toPandas().astype(str)
total_records = len(df_bronze)
print(f"Ingested {total_records} records with {len(df_bronze.columns)} columns.")
display(df_bronze.head(10))

# COMMAND ----------

# MAGIC %md ## 2. Silver Layer — Quality Checks & Cleaning

# COMMAND ----------

df = df_bronze.copy()
reasons = pd.Series([""] * len(df), index=df.index)

def flag(mask, reason):
    reasons[mask] = reasons[mask].apply(lambda x: x + "; " if x else "") + reason

def non_numeric_mask(series):
    def _bad(v):
        try:   float(str(v).strip()); return False
        except: return True
    return series.apply(_bad)

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

# Identify always-null columns (exclude from missing check)
always_null_cols = [c for c in df.columns if df[c].isnull().all()]
check_cols       = [c for c in df.columns if c not in always_null_cols]

# Input test 1: missing values
missing_mask = (
    df[check_cols].isnull().any(axis=1)
    | (df[check_cols].apply(lambda c: c.str.strip() == "")).any(axis=1)
)
flag(missing_mask, "missing_values")
print(f"Test 1 — Missing values: {missing_mask.sum()} rows")

# Input test 2: type mismatches
for col in NUMERIC_COLS_RAW:
    if col in df.columns:
        bad = non_numeric_mask(df[col])
        if bad.any():
            flag(bad, f"type_mismatch:{col}")
            print(f"Test 2 — Type mismatch \'{col}\': {bad.sum()} rows  {df.loc[bad, col].unique().tolist()}")

# Input test 3: invalid domain values
bad_it = ~df["issue_type"].isin(VALID_ISSUE_TYPES)
flag(bad_it, "invalid_value:issue_type")
print(f"Test 3 — Invalid issue_type: {bad_it.sum()} rows")

bad_ra = ~to_num(df["resting_analysis_results"]).isin(VALID_RESTING)
flag(bad_ra, "invalid_value:resting_analysis_results")
print(f"Test 3 — Invalid resting_res: {bad_ra.sum()} rows")

bad_pm = ~to_num(df["pist_m"]).isin(VALID_PIST_M)
flag(bad_pm, "invalid_value:pist_m")

for bc in ["past_dmg", "full_load_issues"]:
    bad_b = ~to_num(df[bc]).isin(VALID_BOOL_VALS)
    flag(bad_b, f"invalid_value:{bc}")

rejected_mask = reasons != ""
df_rejected   = df[rejected_mask].copy()
df_rejected["rejection_reason"] = reasons[rejected_mask].values

print(f"\nTotal rejected: {rejected_mask.sum()} / {total_records}")
print(f"Passing records: {(~rejected_mask).sum()}")

# Build Silver
df_silver = df[~rejected_mask].copy().rename(columns=COLUMN_RENAME)
int_cols = [v for k, v in COLUMN_RENAME.items() if k != "issue_type"]
for c in int_cols:
    if c in df_silver.columns:
        df_silver[c] = pd.to_numeric(df_silver[c], errors="coerce").astype("Int64")
df_silver["combustion_issue_type"] = df_silver["combustion_issue_type"].astype(str).str.strip()

print(f"\nSilver shape: {df_silver.shape}")
display(df_silver.head(10))

# COMMAND ----------

# MAGIC %md ## 3. Gold Layer — Semantic & Business Filtered

# COMMAND ----------

# Business test: OPH <= 120,000
biz_fail    = df_silver["operating_hours"] > OPH_BUSINESS_LIMIT
df_gold_pre = df_silver[~biz_fail].copy()
print(f"Business test failures (OPH > {OPH_BUSINESS_LIMIT}): {biz_fail.sum()} rows")

# Drop non-informative columns
cols_to_drop = []
for col in df_gold_pre.columns:
    null_pct = df_gold_pre[col].isna().mean()
    n_unique = df_gold_pre[col].nunique(dropna=True)
    if null_pct == 1.0 or n_unique <= 1:
        cols_to_drop.append(col)
        print(f"  Dropping \'{col}\'")

df_gold = df_gold_pre.drop(columns=cols_to_drop)
print(f"\nGold shape: {df_gold.shape}")
print(f"Gold columns: {list(df_gold.columns)}")
display(df_gold.head(10))

# COMMAND ----------

# MAGIC %md ## 4. Summary & Stats

# COMMAND ----------

print("=" * 50)
print(f"  Bronze   : {len(df_bronze)} records  {len(df_bronze.columns)} cols")
print(f"  Silver   : {len(df_silver)} records  {len(df_silver.columns)} cols")
print(f"  Gold     : {len(df_gold)} records  {len(df_gold.columns)} cols")
print(f"  Rejected : {len(df_rejected)} records")
print("=" * 50)

print("\n-- Issue type distribution (gold) --")
display(df_gold["combustion_issue_type"].value_counts().reset_index())

print("\n-- High breakdown risk (gold) --")
display(df_gold["high_breakdown_risk"].value_counts().reset_index())

print("\n-- OPH stats (gold) --")
display(df_gold["operating_hours"].describe().reset_index())