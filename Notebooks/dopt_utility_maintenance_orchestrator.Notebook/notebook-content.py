# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # dopt_utility_maintenance_orchestrator
# ## Purpose
# Iterates all tables in a Lakehouse and runs OPTIMIZE (if needed) and VACUUM (weekly or
# forced) on each. Designed to be scheduled as a standalone pipeline, or used as a
# starting point before per-table pipeline calls are in place.
# ## What it does
# - Lists all tables in the Lakehouse via `SHOW TABLES`
# - Runs OPTIMIZE on every table whose average file size is below the target threshold -
#   healthy tables are skipped automatically
# - Runs VACUUM on Sundays (or immediately if `force_vacuum = True`)
# - Catches and logs errors per table - one failing table does not stop the run
# - Prints a summary of tables optimized, skipped, vacuumed, and errored
# ## When to use this vs dopt_utility_table_maintenance
# Use this orchestrator when you want Lakehouse-wide coverage in a single pipeline step.
# Once pipelines are mature, prefer calling `dopt_utility_table_maintenance` as the final
# step of each individual pipeline load - that ties maintenance to the natural cadence of
# each table's data changes, and avoids running across the whole Lakehouse every time.
# ## Prerequisites
# - This notebook must be called from a Fabric pipeline via the Notebook activity
# - The Lakehouse GUID must be passed as a parameter
# - For Gold tables serving Power BI Direct Lake: ensure this notebook completes **before**
#   the Power BI dataset refresh is triggered
# # ## First run on a neglected Lakehouse
# If this notebook is being run for the first time on a Lakehouse that has not previously
# had maintenance applied, the first OPTIMIZE run may be expensive — small files will have
# accumulated across every table and all of them will fall below the compaction threshold.
# This is expected and normal. Subsequent runs will be significantly cheaper: once tables
# are healthy, Fast Optimize skips bins that do not need compaction and most tables will
# be skipped entirely.


# PARAMETERS CELL ********************

# -- Parameters ----------------------------------------------------------------
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

lakehouse_guid = ""     # The GUID of the Lakehouse to maintain
target_mb      = 400    # Target average file size in MB - 256 for Silver, 400 for Gold
force_vacuum   = False  # Set True to trigger VACUUM regardless of day of week

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# | Parameter | Type | Description |
# |---|---|---|
# | `lakehouse_guid` | string | The GUID of the Lakehouse to maintain. Found in the Lakehouse URL in the Fabric portal |
# | `target_mb` | integer | Target average Parquet file size in MB. Use **256** for Silver, **400** for Gold |
# | `force_vacuum` | boolean | When `True`, VACUUM runs on all tables regardless of day. Use after large backfills or initial loads. Default: `False` |


# MARKDOWN ********************

# ## Validation


# CELL ********************

# -- Validation ----------------------------------------------------------------

if not lakehouse_guid:
    raise ValueError("Parameter 'lakehouse_guid' is required but was not provided.")

print(f"Lakehouse       : {lakehouse_guid}")
print(f"Target file size: {target_mb} MB")
print(f"Force VACUUM    : {force_vacuum}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Functions
# Two functions are defined below. They are called in the orchestration cell - do not
# modify the function definitions unless you intend to change the maintenance logic globally.


# CELL ********************

# -- Functions -----------------------------------------------------------------

def optimize_if_needed(fully_qualified_name, target_mb=400, tolerance=0.8):
    """
    Runs OPTIMIZE on a Delta table only if the average file size is meaningfully
    below the target. Tables already at or near the target are skipped.

    Returns a dict with result ("optimized" or "skipped") and, when optimized,
    files_before, files_after, and files_compacted for summary reporting.
    """
    details_before   = spark.sql(f"DESCRIBE DETAIL {fully_qualified_name}").collect()[0]
    num_files_before = details_before['numFiles']

    if num_files_before == 0:
        print(f"  {fully_qualified_name}: skipped - no files")
        return {"result": "skipped"}

    avg_mb_before = (details_before['sizeInBytes'] / num_files_before) / (1024**2)
    threshold_mb  = target_mb * tolerance

    if avg_mb_before >= threshold_mb:
        print(f"  {fully_qualified_name}: skipped - avg {avg_mb_before:.0f}MB is within tolerance")
        return {"result": "skipped"}

    spark.sql(f"OPTIMIZE {fully_qualified_name}")

    details_after   = spark.sql(f"DESCRIBE DETAIL {fully_qualified_name}").collect()[0]
    num_files_after = details_after['numFiles']
    avg_mb_after    = (details_after['sizeInBytes'] / num_files_after) / (1024**2) if num_files_after > 0 else 0
    files_compacted = num_files_before - num_files_after

    print(f"  {fully_qualified_name}: OPTIMIZE ran - files {num_files_before:,} → {num_files_after:,} ({files_compacted:,} compacted) | avg {avg_mb_before:.0f}MB → {avg_mb_after:.0f}MB")

    return {
        "result":          "optimized",
        "files_before":    num_files_before,
        "files_after":     num_files_after,
        "files_compacted": files_compacted,
    }


def vacuum_table(fully_qualified_name, retain_hours=168):
    """
    Runs VACUUM on a Delta table. Never runs below 168 hours (7 days) - the minimum
    safe retention to protect concurrent readers and Direct Lake framing.
    """
    spark.sql(f"VACUUM {fully_qualified_name} RETAIN {retain_hours} HOURS")
    print(f"  {fully_qualified_name}: VACUUM ran - retained {retain_hours}h")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Orchestration
# Iterates all tables in the Lakehouse. OPTIMIZE runs on every table that needs it.
# VACUUM runs on Sundays or when `force_vacuum = True`.
# Errors on individual tables are caught and logged - the run continues regardless.


# CELL ********************

# -- Orchestration -------------------------------------------------------------

from datetime import datetime

run_vacuum = force_vacuum or datetime.today().weekday() == 6  # 6 = Sunday

tables = spark.sql(f"SHOW TABLES IN {lakehouse_guid}").collect()

optimized_count      = 0
skipped_count        = 0
vacuumed_count       = 0
error_count          = 0
files_compacted_total = 0

print(f"Tables found : {len(tables)}")
print(f"VACUUM active: {run_vacuum}")
print("-" * 60)

for row in tables:
    table_name           = row.tableName
    fully_qualified_name = f"{lakehouse_guid}.{table_name}"

    try:
        result = optimize_if_needed(fully_qualified_name, target_mb=target_mb)
        if result["result"] == "optimized":
            optimized_count       += 1
            files_compacted_total += result.get("files_compacted", 0)
        else:
            skipped_count += 1

        if run_vacuum:
            vacuum_table(fully_qualified_name)
            vacuumed_count += 1

    except Exception as e:
        print(f"  {fully_qualified_name}: ERROR - {str(e)}")
        error_count += 1

print("-" * 60)
print(f"Summary - optimized: {optimized_count} | skipped: {skipped_count} | vacuumed: {vacuumed_count} | errors: {error_count} | files compacted: {files_compacted_total:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
