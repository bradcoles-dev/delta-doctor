# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # doctor_diagnosis_table_health
# ## Purpose
# Scans all tables in a Lakehouse and produces a health report — file counts,
# average file sizes, fragmentation status, deletion vector state, and clustering
# configuration. Run this notebook interactively before enabling maintenance for the
# first time, or any time you want to understand the current state of your tables
# before making changes.
# ## What it does
# - Enumerates tables via the OneLake ABFSS path — no Lakehouse attachment required
# - Reads table metadata via `DESCRIBE DETAIL` for every table — no data is scanned
# - Runs in seconds regardless of table size or row count
# - Flags tables that need OPTIMIZE, are borderline, or are already healthy
# - Surfaces partitioned tables (candidates for liquid clustering migration) and tables
#   without deletion vectors enabled
# - Handles both schema-enabled and non-schema Lakehouses automatically
# ## Custom mode
# Pass `layer = "custom"` to assess tables against a non-standard target file size. Set
# `custom_target_mb` to your target in MB — status classification uses that value instead
# of a layer default. Leave `custom_target_mb = 0` to see raw metrics with no status
# classification (status shows as `No target set`). Use custom mode for Lakehouses that
# do not follow Bronze / Silver / Gold, or to assess tables against a non-standard target.
# ## Prerequisites
# - `lakehouse_guid` must be provided (see Parameters below)
# - This notebook must reside in the same Fabric workspace as the target Lakehouse
# - Set `layer` to match the layer you are assessing before running. If assessing an
#   unclassified or mixed Lakehouse for the first time, use `layer = "custom"` with
#   `custom_target_mb = 0` to see raw metrics without status classification


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

lakehouse_guid   = ""       # The GUID of the Lakehouse to scan. Found in the Lakehouse URL in the Fabric portal
layer            = "silver" # Medallion layer: "bronze", "silver", "gold", or "custom"
custom_target_mb = 0        # Custom mode only: target file size in MB for status classification. 0 to skip status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# | Parameter | Type | Description |
# |---|---|---|
# | `lakehouse_guid` | string | The GUID of the Lakehouse to scan. Found in the Lakehouse URL in the Fabric portal |
# | `layer` | string | Medallion layer being assessed. Accepts `"bronze"`, `"silver"`, `"gold"`, or `"custom"`. Default: `"silver"` |
# | `custom_target_mb` | integer | **Custom mode only.** Target file size in MB for status classification. `0` to omit status classification |
# ## Reading the Output
# | Column | What it tells you |
# |---|---|
# | `schema` | Schema name for schema-enabled Lakehouses; empty for non-schema Lakehouses |
# | `table` | Table name |
# | `num_files` | High file count with low average size is the small files problem in numbers |
# | `size_gb` | Total logical size of the table |
# | `avg_file_mb` | Compare against the layer target — below 50% of target is a priority |
# | `partitioned` | Partitioned tables are candidates for liquid clustering migration |
# | `liquid_clustering` | Whether the table has a liquid clustering policy defined |
# | `deletion_vectors` | Tables without deletion vectors enabled are candidates for enabling |
# | `status` | Triage priority — sort ascending to start from the tables that need the most work |
# **Status values** (in evaluation order):
# - `Skip - empty table` — table has no files; may be freshly created or fully vacuumed
# - `Skip - single file` — table has one file; nothing to compact (trivially small or already fully compacted)
# - `No target set` — custom mode with no target specified; raw metrics only
# - `Oversized` — average file size exceeds 2× target; run `doctor_treatment_rebaseline_orchestrator` to rewrite and right-size all files. Note: average file size is a proxy — a bimodal mix of small and large files may not be detected
# - `Healthy` — average file size is at or above target and within 2× target. Note: average file size is a proxy — a bimodal mix of small and large files may appear Healthy on average
# - `Review` — average file size is between 50% and 100% of target; monitor. Tables between 50% and 80% of target will receive OPTIMIZE from the maintenance notebooks; tables between 80% and 100% will be skipped
# - `Needs OPTIMIZE` — average file size is below 50% of target; priority


# MARKDOWN ********************

# ## Validation


# CELL ********************

# ── Validation ────────────────────────────────────────────────────────────────

valid_layers  = {"bronze", "silver", "gold", "custom"}
LAYER_TARGETS = {"bronze": 128, "silver": 256, "gold": 400}

if not lakehouse_guid:
    raise ValueError("Parameter 'lakehouse_guid' is required but was not provided.")

if not layer or layer.lower() not in valid_layers:
    raise ValueError(f"Parameter 'layer' must be one of: {', '.join(sorted(valid_layers))}. Got: '{layer}'")

layer     = layer.lower()
target_mb = LAYER_TARGETS.get(layer) or (custom_target_mb if custom_target_mb > 0 else None)

if layer != "custom" and custom_target_mb > 0:
    print(f"Note: 'custom_target_mb' is ignored when layer is '{layer}' — using {target_mb} MB layer default.")

workspace_guid = spark.conf.get("trident.workspace.id")

print(f"Lakehouse: {lakehouse_guid}")
print(f"Layer    : {layer}")
if target_mb:
    print(f"Target   : {target_mb} MB")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Table Health Scan


# CELL ********************

# ── Table health scan ─────────────────────────────────────────────────────────

from pyspark.sql import functions as F, types as T


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
            sub_items  = mssparkutils.fs.ls(item.path)
            sub_names  = [s.name.rstrip('/') for s in sub_items]
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


tables      = list_delta_tables(workspace_guid, lakehouse_guid)
results     = []
error_count = 0

for entry in tables:
    schema_val   = entry["schema"]
    table_name   = entry["table"]
    table_path   = entry["path"]
    display_name = f"{schema_val}.{table_name}" if schema_val else table_name
    try:
        d          = spark.sql(f"DESCRIBE DETAIL '{table_path}'").first()
        num_files  = d.numFiles or 0
        size_bytes = d.sizeInBytes or 0
        avg_mb     = round(size_bytes / num_files / 1_048_576, 1) if num_files > 0 else 0
        props      = d.properties or {}

        dv_enabled        = str(props.get("delta.enableDeletionVectors", "false")).lower() == "true"
        partitioned       = bool(d.partitionColumns)
        liquid_clustering = bool(getattr(d, "clusteringColumns", None))

        if num_files == 0:
            status = "Skip - empty table"
        elif num_files == 1:
            status = "Skip - single file"
        elif target_mb is None:
            status = "No target set"
        elif avg_mb > target_mb * 2:
            status = "Oversized"
        elif avg_mb >= target_mb:
            status = "Healthy"
        elif avg_mb >= target_mb / 2:
            status = "Review"
        else:
            status = "Needs OPTIMIZE"

        results.append((
            schema_val, table_name, num_files,
            round(size_bytes / 1_073_741_824, 3),
            avg_mb, partitioned, liquid_clustering, dv_enabled, status
        ))
    except Exception as e:
        results.append((schema_val, table_name, None, None, None, None, None, None, f"Error: {str(e)}"))
        error_count += 1

schema = T.StructType([
    T.StructField("schema",           T.StringType()),
    T.StructField("table",            T.StringType()),
    T.StructField("num_files",        T.LongType()),
    T.StructField("size_gb",          T.DoubleType()),
    T.StructField("avg_file_mb",      T.DoubleType()),
    T.StructField("partitioned",      T.BooleanType()),
    T.StructField("liquid_clustering",T.BooleanType()),
    T.StructField("deletion_vectors", T.BooleanType()),
    T.StructField("status",           T.StringType()),
])

display(
    spark.createDataFrame(results, schema=schema)
    .orderBy(F.col("avg_file_mb").asc_nulls_last())
)

if error_count > 0:
    print(f"\nWarning: {error_count} table(s) returned errors — scroll to the bottom of the results to review.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
