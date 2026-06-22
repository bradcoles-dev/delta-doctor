# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # doctor_treatment_rebaseline_orchestrator
# ## Purpose
# Runs a one-off rebaseline across all tables in a Lakehouse. Designed to be run
# once on a Lakehouse that has not previously had Delta maintenance applied — or any
# time you need to reset file sizes back to the correct layer target after prolonged
# neglect or a change in target configuration.
# ## What it does
# - Enumerates all tables via the OneLake ABFSS path — handles both schema-enabled and
#   non-schema Lakehouses automatically
# - Runs `REORG TABLE APPLY (PURGE)` on every table, rewriting only files that contain
#   deletion vectors and purging them
# - Immediately follows each REORG with OPTIMIZE to right-size files to the layer target
# - Catches and logs errors per table — one failing table does not stop the run
# - Prints before/after file counts and average file size per table, and a run summary
# ## When to use this
# Run this notebook as part of the onboarding sequence — designed for one-off use, but
# safe to re-run if needed (a second run on an already-baselined Lakehouse is low-cost:
# REORG finds no deletion vectors to purge, and OPTIMIZE completes quickly — Fast Optimize
# skips bins that do not need compaction):
# 1. Run `doctor_prevention_set_properties_orchestrator` to set `delta.targetFileSize` and
#    other table properties across all tables
# 2. Run this notebook to rebaseline file sizes across the Lakehouse
# 3. Switch to `doctor_treatment_maintenance_orchestrator` (or `doctor_treatment_table_maintenance`
#    per pipeline) for ongoing maintenance going forward
# Do not include this notebook in a recurring pipeline — use `doctor_treatment_maintenance_orchestrator`
# for ongoing scheduled maintenance instead.
# ## Warning — expensive operation
# `REORG TABLE APPLY (PURGE)` rewrites files that contain deletion vectors — on a heavily
# updated or previously unmaintained Lakehouse this may be a large portion of each table.
# OPTIMIZE then right-sizes all files to the layer target. Together these two operations
# can take significant time — expect at least minutes per table. Monitor in the Spark UI.
# ## Warning — deletion vectors and protocol upgrade
# Deletion vectors were enabled by `doctor_prevention_set_properties_orchestrator` in the
# previous step. If you have not yet verified that all clients reading these tables support
# deletion vectors, do not proceed — see `docs/deletion-vectors.md`.
# If deletion vectors have not been enabled on a table, REORG has nothing to purge —
# OPTIMIZE still runs and right-sizes files to the layer target.
# ## Prerequisites
# - `doctor_prevention_set_properties_orchestrator` must have been run first to set
#   `delta.targetFileSize` as a table property — this gives ATFS the per-table ceiling
#   it needs to right-size files correctly during OPTIMIZE
# - This notebook must reside in the same Fabric workspace as the target Lakehouse
# ## Post-rebaseline VACUUM
# REORG TABLE APPLY (PURGE) rewrites files containing deletion vectors, but the original
# files remain on disk until VACUUM removes them. After rebaseline completes, schedule a
# VACUUM pass via `doctor_treatment_maintenance_orchestrator` with `force_vacuum = True` —
# but only after the 7-day retention window has elapsed from the rebaseline run date.
# Running VACUUM before 7 days risks removing files still referenced by open transactions.
# ## One Lakehouse per layer
# This notebook assumes one Lakehouse per medallion layer. Run it once per Lakehouse,
# passing the matching `layer` parameter each time:
# - Bronze Lakehouse → `layer = "bronze"`
# - Silver Lakehouse → `layer = "silver"`
# - Gold Lakehouse   → `layer = "gold"`


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

lakehouse_guid = ""        # The GUID of the Lakehouse to rebaseline
layer          = "silver"  # Medallion layer: "bronze", "silver", or "gold". Must match the layer of all tables in this Lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# | Parameter | Type | Description |
# |---|---|---|
# | `lakehouse_guid` | string | The GUID of the Lakehouse to rebaseline. Found in the Lakehouse URL in the Fabric portal |
# | `layer` | string | The medallion layer for all tables in this Lakehouse. Accepts `"bronze"`, `"silver"`, or `"gold"`. `"custom"` is not supported — all tables in a Lakehouse share the same layer. Default: `"silver"` |


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

workspace_guid = mssparkutils.env.getWorkspaceId()

print(f"Lakehouse       : {lakehouse_guid}")
print(f"Layer           : {layer}")
print(f"Target file size: {target_mb} MB")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Functions
# Two functions are defined below. `list_delta_tables()` enumerates all Delta tables in
# the Lakehouse. `rebaseline_table()` runs REORG TABLE APPLY (PURGE) followed by OPTIMIZE
# on a single table.


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


def rebaseline_table(table_path, display_name):
    """
    Runs REORG TABLE APPLY (PURGE) followed by OPTIMIZE on a Delta table.
    REORG rewrites only files that contain deletion vectors and purges them.
    OPTIMIZE right-sizes all files to the layer target via ATFS.
    Skips empty tables.
    """
    details_before   = spark.sql(f"DESCRIBE DETAIL '{table_path}'").collect()[0]
    num_files_before = details_before['numFiles']

    if num_files_before == 0:
        print(f"  {display_name}: skipped — no files")
        return {"result": "skipped"}

    avg_mb_before = (details_before['sizeInBytes'] / num_files_before) / (1024**2)

    spark.sql(f"REORG TABLE delta.`{table_path}` APPLY (PURGE)")
    print(f"  {display_name}: REORG complete")
    spark.sql(f"OPTIMIZE '{table_path}'")

    details_after   = spark.sql(f"DESCRIBE DETAIL '{table_path}'").collect()[0]
    num_files_after = details_after['numFiles']
    avg_mb_after    = (details_after['sizeInBytes'] / num_files_after) / (1024**2) if num_files_after > 0 else 0
    files_compacted = num_files_before - num_files_after

    print(f"  {display_name}: rebaselined — files {num_files_before:,} → {num_files_after:,} ({files_compacted:,} compacted) | avg {avg_mb_before:.0f}MB → {avg_mb_after:.0f}MB")

    return {
        "result":          "rebaselined",
        "files_before":    num_files_before,
        "files_after":     num_files_after,
        "files_compacted": files_compacted,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Rebaseline
# Iterates all tables in the Lakehouse and runs `rebaseline_table()` on each.
# Errors on individual tables are caught and logged — the run continues regardless.
# A summary is printed at the end.


# CELL ********************

# ── Rebaseline ────────────────────────────────────────────────────────────────

tables = list_delta_tables(workspace_guid, lakehouse_guid)

rebaselined_count     = 0
skipped_count         = 0
error_count           = 0
files_compacted_total = 0

print(f"Tables found    : {len(tables)}")
print(f"Target file size: {target_mb} MB")
print(f"Note            : all tables use the {layer} target ({target_mb} MB). Designed for one-off use — safe to re-run if needed.")
print("-" * 60)

for entry in tables:
    table_path   = entry["path"]
    display_name = f"{entry['schema']}.{entry['table']}" if entry["schema"] else entry["table"]

    try:
        result = rebaseline_table(table_path, display_name)
        if result["result"] == "rebaselined":
            rebaselined_count     += 1
            files_compacted_total += result.get("files_compacted", 0)
        else:
            skipped_count += 1
    except Exception as e:
        print(f"  {display_name}: ERROR — {str(e)}")
        error_count += 1

print("-" * 60)
print(f"Summary — rebaselined: {rebaselined_count} | skipped: {skipped_count} | errors: {error_count} | files compacted: {files_compacted_total:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
