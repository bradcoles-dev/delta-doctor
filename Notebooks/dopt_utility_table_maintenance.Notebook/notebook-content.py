# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # dopt_utility_table_maintenance
# ## Purpose
# Performs Delta table maintenance (OPTIMIZE and VACUUM) on a single table, passed as a
# parameter from the calling pipeline.
# This notebook is designed to run as the **final step of a data pipeline**, after the
# ingestion or transformation notebook for a given table has completed. It is called from
# a Fabric pipeline using the Notebook activity, with parameters passed at runtime.
# ## What it does
# - **OPTIMIZE**: Compacts small Parquet files into larger, right-sized files.
#   Only runs if the average file size is meaningfully below the target — tables with little
#   or no recent change (e.g. dimension tables, lookup tables) are automatically skipped.
# - **VACUUM**: Removes Parquet files no longer referenced by the Delta transaction log
#   (orphaned files from previous writes, updates, and compaction runs). Runs weekly by
#   default (Sundays), or on demand if forced via pipeline parameter.
# ## Why this matters
# Microsoft Fabric capacity is billed by SKU tier, and each tier doubles in cost. Without
# regular maintenance, Delta tables accumulate small files and unresolved soft-deletes,
# causing queries to scan far more data than necessary. This silently inflates capacity
# consumption over time — the difference between a well-maintained platform and a neglected
# one can be the difference between your current SKU and the next one up.
# ## Prerequisites
# - This notebook must be called from a Fabric pipeline via the Notebook activity
# - Parameters must be passed by the pipeline (see Parameters cell below)
# - This notebook must reside in the same Fabric workspace as the target Lakehouse
# - For Gold tables serving Power BI Direct Lake: ensure this notebook completes **before**
#   the Power BI dataset refresh is triggered


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

lakehouse_guid   = ""        # The GUID of the Lakehouse containing the target table
table_name       = ""        # The table name (without schema prefix), e.g. "fact_sales"
schema_name      = ""        # Schema name for schema-enabled Lakehouses. Leave empty for non-schema Lakehouses
layer            = "silver"  # Medallion layer: "bronze", "silver", "gold", or "custom"
custom_target_mb = 0         # Custom mode only: target file size in MB for OPTIMIZE gating. Required when layer = "custom"
force_vacuum     = False     # Set True in the pipeline to trigger VACUUM outside the weekly schedule

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# | Parameter | Type | Description |
# |---|---|---|
# | `lakehouse_guid` | string | The GUID of the Lakehouse containing the target table. Found in the Lakehouse URL in the Fabric portal |
# | `table_name` | string | The name of the table to maintain, without schema prefix (e.g. `fact_sales`) |
# | `schema_name` | string | Schema name for schema-enabled Lakehouses (e.g. `silver`). Leave empty for Lakehouses without schemas |
# | `layer` | string | Medallion layer for the table. Accepts `"bronze"`, `"silver"`, `"gold"`, or `"custom"`. Default: `"silver"` |
# | `custom_target_mb` | integer | **Custom mode only.** Target file size in MB for OPTIMIZE gating. Required when `layer = "custom"` |
# | `force_vacuum` | boolean | When `True`, VACUUM runs regardless of the day of the week. Use for ad-hoc runs after large backfills or initial loads. Default: `False` |
# ### Why different targets per layer?
# - **Silver (256 MB):** Intermediate layer — balances write and read performance for Spark processing
# - **Gold (400 MB):** Consumption layer — the SQL Analytics Endpoint and Power BI Direct Lake
#   perform best with files in the 400 MB–1 GB range. Larger files mean fewer files to scan per query.
# ### How to set parameters in the pipeline
# In the Fabric pipeline, add a Notebook activity pointing to this notebook. In the activity's
# **Settings** tab, expand **Base parameters** and add each parameter with its runtime value.
# The calling pipeline knows which table it has just loaded and which layer it belongs to —
# pass those values directly.


# MARKDOWN ********************

# ## Validation
# Confirms that required parameters have been provided before attempting any maintenance
# operations. If either `lakehouse_guid` or `table_name` is empty, the notebook exits early
# with a clear error message rather than failing mid-execution.

# CELL ********************


# ── Validation ────────────────────────────────────────────────────────────────

valid_layers  = {"bronze", "silver", "gold", "custom"}
LAYER_TARGETS = {"bronze": 128, "silver": 256, "gold": 400}

if not lakehouse_guid:
    raise ValueError("Parameter 'lakehouse_guid' is required but was not provided.")

if not table_name:
    raise ValueError("Parameter 'table_name' is required but was not provided.")

if not layer or layer.lower() not in valid_layers:
    raise ValueError(f"Parameter 'layer' must be one of: {', '.join(sorted(valid_layers))}. Got: '{layer}'")

layer = layer.lower()

if layer == "custom":
    if not custom_target_mb or custom_target_mb <= 0:
        raise ValueError("Parameter 'custom_target_mb' must be a positive integer when layer = 'custom'.")
    target_mb = custom_target_mb
else:
    target_mb = LAYER_TARGETS[layer]

workspace_guid = mssparkutils.env.getWorkspaceId()
onelake_base   = f"abfss://{workspace_guid}@onelake.dfs.fabric.microsoft.com/{lakehouse_guid}/Tables"
table_path     = f"{onelake_base}/{schema_name}/{table_name}" if schema_name else f"{onelake_base}/{table_name}"
display_name   = f"{schema_name}.{table_name}" if schema_name else table_name

print(f"Target table    : {display_name}")
print(f"Layer           : {layer}")
print(f"Target file size: {target_mb} MB")
print(f"Force VACUUM    : {force_vacuum}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Functions
# Two functions are defined below. They are called in later cells — do not modify the
# function definitions unless you intend to change the maintenance logic globally.

# CELL ********************


# ── Functions ─────────────────────────────────────────────────────────────────

def optimize_if_needed(table_path, display_name, target_mb=400, tolerance=0.8):
    """
    Runs OPTIMIZE on a Delta table only if the average file size is meaningfully
    below the target. Tables already at or near the target are skipped, avoiding
    unnecessary compute spend on tables with little or no recent change.

    Args:
        table_path   (str):   ABFSS path to the Delta table
        display_name (str):   Human-readable name for log output
        target_mb    (int):   Target average file size in MB. Default: 400 MB
        tolerance  (float):   Fraction of target below which OPTIMIZE is triggered.
                              Default: 0.8 (trigger if avg < 80% of target)

    Returns a dict with result ("optimized" or "skipped") and, when optimized,
    files_before, files_after, and files_compacted for summary reporting.

    How it works:
        1. Reads table metadata via DESCRIBE DETAIL (fast — no data scan)
        2. Calculates average file size across all current Parquet files
        3. If average is below the tolerance threshold, runs OPTIMIZE
        4. Microsoft's Fast Optimize (enabled in the session config notebook) handles
           bin-level evaluation within the run — skipping file groups that do not
           need compaction
    """
    details_before   = spark.sql(f"DESCRIBE DETAIL '{table_path}'").collect()[0]
    num_files_before = details_before['numFiles']

    if num_files_before == 0:
        print(f"{display_name}: skipped — no files")
        return {"result": "skipped"}

    if num_files_before == 1:
        print(f"{display_name}: skipped — single file, nothing to compact")
        return {"result": "skipped"}

    avg_mb_before = (details_before['sizeInBytes'] / num_files_before) / (1024**2)
    threshold_mb  = target_mb * tolerance

    if avg_mb_before >= threshold_mb:
        print(f"{display_name}: skipped — avg {avg_mb_before:.0f}MB is within tolerance of {target_mb}MB target")
        return {"result": "skipped"}

    spark.sql(f"OPTIMIZE '{table_path}'")

    details_after   = spark.sql(f"DESCRIBE DETAIL '{table_path}'").collect()[0]
    num_files_after = details_after['numFiles']
    avg_mb_after    = (details_after['sizeInBytes'] / num_files_after) / (1024**2) if num_files_after > 0 else 0
    files_compacted = num_files_before - num_files_after

    print(f"{display_name}: OPTIMIZE ran")
    print(f"  files    : {num_files_before:,} → {num_files_after:,} ({files_compacted:,} compacted)")
    print(f"  avg size : {avg_mb_before:.0f}MB → {avg_mb_after:.0f}MB")

    return {
        "result":          "optimized",
        "files_before":    num_files_before,
        "files_after":     num_files_after,
        "files_compacted": files_compacted,
    }


def vacuum_table(table_path, display_name, retain_hours=168):
    """
    Runs VACUUM on a Delta table, removing Parquet files no longer referenced
    by the Delta transaction log.

    Args:
        table_path   (str): ABFSS path to the Delta table
        display_name (str): Human-readable name for log output
        retain_hours (int): Retention period in hours. Default: 168 (7 days)

    IMPORTANT — Never set retain_hours below 168 (7 days):
        Delta's 7-day minimum retention exists to protect concurrent readers.
        Any long-running query or streaming job that opened a snapshot before
        the VACUUM run could reference files that no longer exist, causing
        FileNotFoundException errors and potential data corruption.

    IMPORTANT — Direct Lake tables:
        For Gold tables serving Power BI Direct Lake, ensure the semantic model
        has been refreshed (re-framed to the current Delta commit version) before
        VACUUM runs. Direct Lake holds a reference to a specific commit version —
        if VACUUM removes files from that version before the model re-frames,
        users will encounter query errors.
    """
    retain_hours = max(retain_hours, 168)  # 7-day minimum — enforced in code, not just documented
    spark.sql(f"VACUUM '{table_path}' RETAIN {retain_hours} HOURS")
    print(f"{display_name}: VACUUM ran — retained {retain_hours}h")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## OPTIMIZE
# Runs `optimize_if_needed` on the target table on every pipeline execution.
# The table is skipped automatically if its average file size is already within tolerance
# of the target — so tables with little recent change will not incur unnecessary compute.
# ### Why OPTIMIZE matters at Silver and Gold
# Auto-compaction (enabled in the session config notebook) handles fragmentation inline
# after each write, but targets 128 MB files by default. Silver and Gold consumers need
# larger files:
# - SQL Analytics Endpoint: ~400 MB
# - Power BI Direct Lake: 400 MB–1 GB with large row groups
# OPTIMIZE also physically applies Liquid Clustering — data is only clustered when OPTIMIZE
# runs, not during writes. Without it, liquid clustering provides no file-skipping benefit.
# Finally, OPTIMIZE resolves deletion vectors (soft-deletes). For Gold tables serving Direct
# Lake, accumulated deletion vectors add overhead to cold-cache query loading.

# CELL ********************

# ── OPTIMIZE (every run) ───────────────────────────────────────────────────────

optimize_if_needed(table_path, display_name, target_mb=target_mb)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## VACUUM
# Removes Parquet files that are no longer referenced by the Delta transaction log —
# orphaned files left behind by previous writes, updates, deletes, and compaction runs.
# These files are invisible to queries but consume OneLake storage.
# **Runs weekly by default (Sundays), or immediately if `force_vacuum = True`.**
# ### Why weekly and not daily?
# Delta's minimum safe retention period is 7 days (168 hours). Running VACUUM daily would
# remove files written up to 7 days ago — which may still be referenced by:
# - Long-running Spark queries or pipelines that opened a snapshot earlier in the week
# - Power BI Direct Lake models that have not yet re-framed to a newer Delta commit version
# A weekly cadence on Sundays ensures a full 7-day gap between runs, stays within the safe
# retention window, and reclaims storage without risking data access errors.
# ### When to use force_vacuum = True
# Set `force_vacuum = True` in the pipeline parameters to trigger VACUUM outside the weekly
# schedule. Typical use cases:
# - After a large historical backfill or initial data load
# - After running `REORG TABLE ... APPLY (PURGE)` to explicitly resolve deletion vectors
# - During platform decommission or table migration
# ### Direct Lake tables
# Ensure the Power BI semantic model has been refreshed before VACUUM runs. If VACUUM
# removes files from the Delta version the model is still referencing, Direct Lake users
# will encounter query errors until the next model refresh.


# CELL ********************

# ── VACUUM (Sundays or forced) ─────────────────────────────────────────────────

from datetime import datetime

if force_vacuum or datetime.today().weekday() == 6:  # 6 = Sunday
    vacuum_table(table_path, display_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
