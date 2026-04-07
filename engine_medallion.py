# =============================================================================
# Engine Failure Analysis — Medallion Architecture
# =============================================================================
# Run this script in Databricks (attach source_data.csv to DBFS or
# use %run / %pip magic as needed), or locally with Python 3.8+.
#
# Layers:
#   Bronze  — raw ingest, ALL records & columns
#   Silver  — full, valid records only; expanded column names; correct types
#   Gold    — semantic layer; informative columns; passed all quality tests
#
# Input quality tests:
#   Test 1 – missing values
#   Test 2 – type mismatches  (numeric columns contain non-numeric strings)
#   Test 3 – invalid domain values  (issue_type, resting results, booleans)
#
# Business test:
#   OPH <= 120,000
# =============================================================================

import pandas as pd 
import os

# =============================================================================
# 0.  CONFIGURATION
# =============================================================================
SOURCE_FILE         = "source_data.csv"
OPH_BUSINESS_LIMIT  = 120_000

VALID_ISSUE_TYPES   = {"typical", "atypical", "non-related", "non-symptomatic"}
VALID_RESTING       = {0, 1, 2}        # 0=normal, 1=abnormal, 2=critical
VALID_BOOL_VALS     = {0, 1}
VALID_PIST_M        = {0, 1}


NUMERIC_COLS_RAW = [
    "oph", "pist_m", "bmep", "ng_imp", "past_dmg",
    "resting_analysis_results", "rpm_max", "full_load_issues",
    "number_up", "number_tc", "op_set_1", "op_set_3", "high_breakdown_risk",
]
# NOTE: op_set_2 is intentionally omitted — it is 100% NULL in the source
#       and will be dropped as non-informative at the Gold stage.

# Column name expansion map
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


# =============================================================================
# 1.  BRONZE — raw ingest, no transformations
# =============================================================================
print("=" * 60)
print("BRONZE LAYER — raw ingest")
print("=" * 60)

df_bronze = pd.read_csv(SOURCE_FILE, dtype=str)   # ingest everything as string
total_records = len(df_bronze)
print(f"  Records : {total_records}")
print(f"  Columns : {list(df_bronze.columns)}")


# =============================================================================
# 2.  SILVER — quality filtering + rename + cast types
# =============================================================================
print("\n" + "=" * 60)
print("SILVER LAYER — quality checks")
print("=" * 60)

df = df_bronze.copy()

# Track rejection reasons per row
reasons = pd.Series([""] * len(df), index=df.index)

def flag(mask: pd.Series, reason: str) -> None:
    """Append a rejection reason to all flagged rows."""
    reasons[mask] = reasons[mask].apply(lambda x: x + "; " if x else "") + reason

def non_numeric_mask(series: pd.Series) -> pd.Series:
    """Return True where the value cannot be cast to float."""
    def _bad(v):
        try:   float(str(v).strip()); return False
        except: return True
    return series.apply(_bad)

def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

# Identify always-null columns so they don't trigger false missing-value flags
always_null_cols = [c for c in df.columns if df[c].isnull().all()]
check_cols       = [c for c in df.columns if c not in always_null_cols]

# ── Input test 1: missing values ──────────────────────────────────────────────
missing_mask = (
    df[check_cols].isnull().any(axis=1)
    | (df[check_cols].apply(lambda c: c.str.strip() == "")).any(axis=1)
)
flag(missing_mask, "missing_values")
print(f"  Input test 1 – missing values        : {missing_mask.sum()} rows")

# ── Input test 2: type mismatches ─────────────────────────────────────────────
for col in NUMERIC_COLS_RAW:
    if col in df.columns:
        bad = non_numeric_mask(df[col])
        if bad.any():
            flag(bad, f"type_mismatch:{col}")
            print(f"  Input test 2 – type mismatch '{col}' : {bad.sum()} rows  "
                  f"{df.loc[bad, col].unique().tolist()}")

# ── Input test 3: invalid domain values ───────────────────────────────────────
bad_it = ~df["issue_type"].isin(VALID_ISSUE_TYPES)
flag(bad_it, "invalid_value:issue_type")
print(f"  Input test 3 – invalid issue_type    : {bad_it.sum()} rows  "
      f"{df.loc[bad_it, 'issue_type'].dropna().unique().tolist()}")

bad_ra = ~to_num(df["resting_analysis_results"]).isin(VALID_RESTING)
flag(bad_ra, "invalid_value:resting_analysis_results")
print(f"  Input test 3 – invalid resting_res   : {bad_ra.sum()} rows")

bad_pm = ~to_num(df["pist_m"]).isin(VALID_PIST_M)
flag(bad_pm, "invalid_value:pist_m")
print(f"  Input test 3 – invalid pist_m        : {bad_pm.sum()} rows")

for bc in ["past_dmg", "full_load_issues"]:
    bad_b = ~to_num(df[bc]).isin(VALID_BOOL_VALS)
    flag(bad_b, f"invalid_value:{bc}")
    print(f"  Input test 3 – invalid {bc:<22}: {bad_b.sum()} rows")

# ── Rejection log ─────────────────────────────────────────────────────────────
rejected_mask         = reasons != ""
df_rejected           = df[rejected_mask].copy()
df_rejected["rejection_reason"] = reasons[rejected_mask].values
print(f"\n  Total rejected : {rejected_mask.sum()}")
print(f"  Passing        : {(~rejected_mask).sum()}")

# ── Build Silver ──────────────────────────────────────────────────────────────
df_silver = df[~rejected_mask].copy().rename(columns=COLUMN_RENAME)

INT_COLS_SILVER = [
    "operating_hours", "piston_material",
    "brake_mean_effective_pressure", "natural_gas_impurities",
    "past_damage", "resting_analysis_results", "rpm_maximum",
    "full_load_issues", "number_of_unplanned_events",
    "number_of_turbo_chargers", "operational_setting_1",
    "operational_setting_2", "operational_setting_3", "high_breakdown_risk",
]
for c in INT_COLS_SILVER:
    if c in df_silver.columns:
        df_silver[c] = pd.to_numeric(df_silver[c], errors="coerce").astype("Int64")

df_silver["combustion_issue_type"] = (
    df_silver["combustion_issue_type"].astype(str).str.strip()
)
print(f"  Silver shape   : {df_silver.shape}")


# =============================================================================
# 3.  GOLD — semantic layer; informative columns; business test applied
# =============================================================================
print("\n" + "=" * 60)
print("GOLD LAYER — semantic & business filtered")
print("=" * 60)

# Business test
biz_fail = df_silver["operating_hours"] > OPH_BUSINESS_LIMIT
print(f"  Business test – OPH > {OPH_BUSINESS_LIMIT} : {biz_fail.sum()} rows failed")
df_gold_pre = df_silver[~biz_fail].copy()

# Drop non-informative columns (100% null OR single distinct value)
cols_to_drop = []
for col in df_gold_pre.columns:
    null_pct = df_gold_pre[col].isna().mean()
    n_unique = df_gold_pre[col].nunique(dropna=True)
    if null_pct == 1.0:
        cols_to_drop.append((col, "100% null"))
    elif n_unique <= 1:
        cols_to_drop.append((col, f"{n_unique} distinct value(s)"))

print("  Dropping non-informative columns:")
for col, reason in cols_to_drop:
    print(f"    - {col} : {reason}")

df_gold = df_gold_pre.drop(columns=[c for c, _ in cols_to_drop])
print(f"  Gold shape     : {df_gold.shape}")
print(f"  Gold columns   : {list(df_gold.columns)}")


# =============================================================================
# 4.  SAVE OUTPUT TABLES
# =============================================================================
df_bronze.to_csv("bronze_engine.csv",      index=False)
df_silver.to_csv("silver_engine.csv",      index=False)
df_gold.to_csv("gold_engine.csv",          index=False)
df_rejected.to_csv("rejected_records.csv", index=False)

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  {'Layer':<10} {'Records':>8}  {'Columns':>8}")
print(f"  {'Bronze':<10} {len(df_bronze):>8}  {len(df_bronze.columns):>8}")
print(f"  {'Silver':<10} {len(df_silver):>8}  {len(df_silver.columns):>8}")
print(f"  {'Gold':<10} {len(df_gold):>8}  {len(df_gold.columns):>8}")
print(f"  {'Rejected':<10} {len(df_rejected):>8}")
print("\n  Output files: bronze_engine.csv | silver_engine.csv")
print("                gold_engine.csv   | rejected_records.csv")

# =============================================================================
# 5.  QUALITY STATS (for dashboard reference)
# =============================================================================
print("\n" + "=" * 60)
print("QUALITY STATS")
print("=" * 60)

print("\n-- Rejection reasons --")
reason_exp = (
    df_rejected["rejection_reason"]
    .str.split("; ").explode().str.strip()
    .value_counts().reset_index()
)
reason_exp.columns = ["reason", "count"]
print(reason_exp.to_string(index=False))

print("\n-- Combustion issue type distribution (gold) --")
print(df_gold["combustion_issue_type"].value_counts().to_string())

print("\n-- Operating hours stats (gold) --")
print(df_gold["operating_hours"].describe().to_string())

print("\n-- High breakdown risk (gold) --")
print(df_gold["high_breakdown_risk"].value_counts().to_string())

print("\n-- Resting analysis results (gold) --")
print(df_gold["resting_analysis_results"].value_counts().sort_index().to_string())
