# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # doctor_treatment_maintenance_orchestrator
# ## Purpose
# Iterates all tables in a Lakehouse and runs OPTIMIZE (if needed) and VACUUM (weekly or
# forced) on each. Designed to be scheduled as a standalone pipeline, or used as a
# starting point before per-table pipeline calls are in place.
# ## What it does
# - Enumerates all tables via the OneLake ABFSS path — handles both schema-enabled and
#   non-schema Lakehouses automatically
# - Runs OPTIMIZE on every table whose average file size is below the target threshold —
#   healthy tables are skipped automatically
# - Runs VACUUM on Sundays (or immediately if `force_vacuum = True`)
# - Catches and logs errors per table — one failing table does not stop the run
# - Prints a summary of tables optimized, skipped, vacuumed, and errored
# ## When to use this vs doctor_treatment_table_maintenance
# Use this orchestrator when you want Lakehouse-wide coverage in a single pipeline step.
# Once pipelines are mature, prefer calling `doctor_treatment_table_maintenance` as the final
# step of each individual pipeline load — that ties maintenance to the natural cadence of
# each table's data changes, and avoids running across the whole Lakehouse every time.
# ## Prerequisites
# - The Lakehouse GUID must be passed as a parameter
# - This notebook must reside in the same Fabric workspace as the target Lakehouse
# - For Gold tables serving Power BI Direct Lake: ensure this notebook completes **before**
#   the Power BI dataset refresh is triggered
# ## First run on a neglected Lakehouse
# If this notebook is being run for the first time on a Lakehouse that has not previously
# had maintenance applied, the first OPTIMIZE run will take longer than subsequent runs —
# expect at least minutes per table depending on size and fragmentation. Monitor progress
# in the Spark UI. Subsequent runs cost almost nothing when tables are already healthy:
# once tables are healthy, Fast Optimize skips bins that do not need compaction and most
# tables will be skipped entirely.
# ## Liquid clustering on Spark Runtime 1.3
# On Spark Runtime 1.3, liquid clustered tables trigger a full OPTIMIZE pass on every
# orchestrator run — Z-Cube evaluation is not incremental until Runtime 2.0. If this
# Lakehouse contains liquid clustered tables and you are on Runtime 1.3, expect longer
# run times and higher write amplification on those tables. Runtime 2.0 is strongly
# recommended for Lakehouses with liquid clustered tables.


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

lakehouse_guid = ""        # The GUID of the Lakehouse to maintain
layer          = "silver"  # Medallion layer: "bronze", "silver", or "gold". Must match the layer of all tables in this Lakehouse — all tables share one target
force_vacuum   = False     # Set True to trigger VACUUM regardless of day of week

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
# | `layer` | string | Medallion layer for all tables in this Lakehouse. Accepts `"bronze"`, `"silver"`, or `"gold"`. `"custom"` is not supported — all tables in a Lakehouse share the same layer. Default: `"silver"`. For Lakehouses where individual tables need different target sizes, call `doctor_treatment_table_maintenance` directly with `layer = "custom"` |
# | `force_vacuum` | boolean | When `True`, VACUUM runs on all tables regardless of day. Use after large backfills or initial loads. Default: `False` |


# MARKDOWN ********************

# ## Validation


# CELL ********************

# ── Validation ────────────────────────────────────────────────────────────────

valid_layers  = {"bronze", "silver", "gold"}
LAYER_TARGETS = {"bronze": 128, "silver": 256, "gold": 400}

if not lakehouse_guid:
    raise ValueError("Parameter 'lakehouse_guid' is required but was not provided.")

if not layer or layer.lower() not in valid_layers:
    raise ValueError(f"Parameter 'layer' must be one of: {', '.join(sorted(valid_layers))}. Got: '{layer}'")

layer     = layer.lower()
target_mb = LAYER_TARGETS[layer]

force_vacuum   = str(force_vacuum).lower() == "true"

workspace_guid = spark.conf.get("trident.workspace.id")

print(f"Lakehouse       : {lakehouse_guid}")
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
# Three functions are defined below. They are called in the orchestration cell — do not
# modify the function definitions unless you intend to change the maintenance logic globally.


# CELL ********************

# ── Functions ─────────────────────────────────────────────────────────────────

def list_delta_tables(workspace_guid, lakehouse_guid):
    """
    Enumerates all Delta tables in a Lakehouse via ABFSS path listing.
    Handles both schema-enabled Lakehouses (Tables/{schema}/{table}) and
    non-schema Lakehouses (Tables/{table}) by checking for _delta_log presence.
    Returns a list of dicts: {"schema": str, "table": str, "path": str}.
    """
    tables_root = f"abfss://{workspace_guid}@onelake.dfs.fabric.microsoft.com/{lakehouse_guid}/Tables"
    result = []
    try:
        top_items = mssparkutils.fs.ls(tables_root)
    except Exception as e:
        raise RuntimeError(f"Could not list Tables directory for Lakehouse {lakehouse_guid}: {e}")

    for item in top_items:
        item_name = item.name.rstrip('/')
        try:
            sub_items = mssparkutils.fs.ls(item.path)
            sub_names = [s.name.rstrip('/') for s in sub_items]
            if "_delta_log" in sub_names:
                result.append({"schema": "", "table": item_name, "path": item.path.rstrip('/')})
            else:
                # Potential schema folder — recurse one level
                for sub_item in sub_items:
                    sub_name = sub_item.name.rstrip('/')
                    try:
                        deep_items = mssparkutils.fs.ls(sub_item.path)
                        deep_names = [d.name.rstrip('/') for d in deep_items]
                        if "_delta_log" in deep_names:
                            result.append({"schema": item_name, "table": sub_name, "path": sub_item.path.rstrip('/')})
                    except Exception as e:
                        print(f"  Warning: could not enumerate {sub_item.path} — skipped ({e})")
        except Exception as e:
            print(f"  Warning: could not enumerate {item.path} — skipped ({e})")
    return result


def optimize_if_needed(table_path, display_name, target_mb=400, tolerance=0.8):
    """
    Runs OPTIMIZE on a Delta table only if the average file size is meaningfully
    below the target. Tables already at or near the target are skipped.

    Returns a dict with result ("optimized" or "skipped") and, when optimized,
    files_before, files_after, and files_compacted for summary reporting.
    """
    details_before   = spark.sql(f"DESCRIBE DETAIL '{table_path}'").collect()[0]
    num_files_before = details_before['numFiles']

    if num_files_before == 0:
        print(f"  {display_name}: skipped — no files")
        return {"result": "skipped"}

    is_clustered  = bool(getattr(details_before, "clusteringColumns", None))
    avg_mb_before = (details_before['sizeInBytes'] / num_files_before) / (1024**2)
    threshold_mb  = target_mb * tolerance

    if not is_clustered:
        if num_files_before == 1:
            print(f"  {display_name}: skipped — single file, nothing to compact")
            return {"result": "skipped"}

        if avg_mb_before > target_mb * 2:
            print(f"  {display_name}: skipped — avg {avg_mb_before:.0f}MB exceeds 2x {target_mb}MB target; run doctor_treatment_rebaseline_orchestrator")
            return {"result": "skipped"}

        if avg_mb_before >= threshold_mb:
            print(f"  {display_name}: skipped — avg {avg_mb_before:.0f}MB is within tolerance of {target_mb}MB target")
            return {"result": "skipped"}

    spark.sql(f"OPTIMIZE '{table_path}'")

    details_after   = spark.sql(f"DESCRIBE DETAIL '{table_path}'").collect()[0]
    num_files_after = details_after['numFiles']
    avg_mb_after    = (details_after['sizeInBytes'] / num_files_after) / (1024**2) if num_files_after > 0 else 0
    files_compacted = num_files_before - num_files_after

    print(f"  {display_name}: OPTIMIZE ran — files {num_files_before:,} → {num_files_after:,} ({files_compacted:,} compacted) | avg {avg_mb_before:.0f}MB → {avg_mb_after:.0f}MB")

    return {
        "result":          "optimized",
        "files_before":    num_files_before,
        "files_after":     num_files_after,
        "files_compacted": files_compacted,
    }


def vacuum_table(table_path, display_name, retain_hours=168):
    """
    Runs VACUUM on a Delta table. Never runs below 168 hours (7 days) — the minimum
    safe retention to protect concurrent readers and Direct Lake framing.
    """
    retain_hours = max(retain_hours, 168)  # 7-day minimum — enforced in code, not just documented
    spark.sql(f"VACUUM '{table_path}' RETAIN {retain_hours} HOURS")
    print(f"  {display_name}: VACUUM ran — retained {retain_hours}h")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Orchestration
# Iterates all tables in the Lakehouse. OPTIMIZE runs on every table that needs it.
# VACUUM runs on Sundays or when `force_vacuum = True`.
# Errors on individual tables are caught and logged — the run continues regardless.


# CELL ********************

# ── Orchestration ─────────────────────────────────────────────────────────────

from datetime import datetime, timezone

is_sunday  = datetime.now(timezone.utc).weekday() == 6  # 6 = Sunday (UTC)
run_vacuum = force_vacuum or is_sunday

tables = list_delta_tables(workspace_guid, lakehouse_guid)

optimized_count       = 0
skipped_count         = 0
vacuumed_count        = 0
error_count           = 0
files_compacted_total = 0

print(f"Tables found : {len(tables)}")
if force_vacuum:
    print(f"VACUUM       : active (force_vacuum=True — confirm this is intentional for this pipeline run)")
elif is_sunday:
    print(f"VACUUM       : active (Sunday UTC)")
else:
    print(f"VACUUM       : suppressed — next run Sunday UTC (set force_vacuum = True to override)")
print(f"Note         : all tables use the {layer} target ({target_mb} MB). For mixed-layer Lakehouses use doctor_treatment_table_maintenance directly.")
print("-" * 60)

for entry in tables:
    table_path   = entry["path"]
    display_name = f"{entry['schema']}.{entry['table']}" if entry["schema"] else entry["table"]

    try:
        result = optimize_if_needed(table_path, display_name, target_mb=target_mb)
        if result["result"] == "optimized":
            optimized_count       += 1
            files_compacted_total += result.get("files_compacted", 0)
        else:
            skipped_count += 1

        if run_vacuum:
            if layer == "gold":
                print(f"  {display_name}: Direct Lake reminder — confirm semantic model refreshed before VACUUM runs")
            vacuum_table(table_path, display_name)
            vacuumed_count += 1

    except Exception as e:
        print(f"  {display_name}: ERROR — {str(e)}")
        error_count += 1

print("-" * 60)
print(f"Summary — optimized: {optimized_count} | skipped: {skipped_count} | vacuumed: {vacuumed_count} | errors: {error_count} | files compacted: {files_compacted_total:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
