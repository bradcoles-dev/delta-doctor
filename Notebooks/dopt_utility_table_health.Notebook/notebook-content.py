# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # dopt_utility_table_health
# # ## Purpose
# Scans all tables in the attached Lakehouse and produces a health report — file counts,
# average file sizes, fragmentation status, deletion vector state, and clustering configuration.
# # Run this notebook interactively before enabling maintenance for the first time, or any
# time you want to understand the current state of your tables before making changes.
# # ## What it does
# - Reads table metadata via `DESCRIBE DETAIL` for every table — no data is scanned
# - Runs in seconds regardless of table size or row count
# - Flags tables that need OPTIMIZE, are borderline, or are already healthy
# - Surfaces partitioned tables (candidates for liquid clustering migration) and tables
#   without deletion vectors enabled
# # ## Prerequisites
# - A Lakehouse must be attached to this notebook
# - Set `target_mb` to match the layer you are assessing before running


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# Set target_mb to match the layer you are assessing:
#   128 = Bronze   256 = Silver   400 = Gold

target_mb = 256     # Target average file size in MB for this layer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# # | Parameter | Type | Description |
# |---|---|---|
# | `target_mb` | integer | Target average Parquet file size in MB for the layer being assessed. Use **128** for Bronze, **256** for Silver, **400** for Gold. Default: `256` |
# # ## Reading the Output
# # | Column | What it tells you |
# |---|---|
# | `num_files` | High file count with low average size is the small files problem in numbers |
# | `avg_file_mb` | Compare against the layer target — below 50% of target is a priority |
# | `size_gb` | Total logical size of the table |
# | `partitioned` | Partitioned tables are candidates for liquid clustering migration |
# | `liquid_clustering` | Whether the table has a liquid clustering policy defined |
# | `deletion_vectors` | Tables without deletion vectors enabled are candidates for enabling |
# | `status` | Triage priority — sort ascending to start from the tables that need the most work |
# # **Status values:**
# - `Needs OPTIMIZE` — average file size is below 50% of target; priority
# - `Review` — average file size is between 50% and 100% of target; monitor
# - `Healthy` — average file size is at or above target
# - `Skip — single file` — table has one file; nothing to compact


# CELL ********************

# ── Table health scan ─────────────────────────────────────────────────────────

from pyspark.sql import functions as F, types as T

tables  = spark.sql("SHOW TABLES").collect()
results = []

for row in tables:
    table_name = row.tableName
    try:
        d          = spark.sql(f"DESCRIBE DETAIL `{table_name}`").first()
        num_files  = d.numFiles or 0
        size_bytes = d.sizeInBytes or 0
        avg_mb     = round(size_bytes / num_files / 1_048_576, 1) if num_files > 0 else 0
        props      = d.properties or {}

        dv_enabled        = str(props.get("delta.enableDeletionVectors", "false")).lower() == "true"
        partitioned       = bool(d.partitionColumns)
        liquid_clustering = bool(getattr(d, "clusteringColumns", None))

        if num_files <= 1:
            status = "Skip — single file"
        elif avg_mb >= target_mb:
            status = "Healthy"
        elif avg_mb >= target_mb / 2:
            status = "Review"
        else:
            status = "Needs OPTIMIZE"

        results.append((
            table_name, num_files,
            round(size_bytes / 1_073_741_824, 3),
            avg_mb, partitioned, liquid_clustering, dv_enabled, status
        ))
    except Exception as e:
        results.append((table_name, None, None, None, None, None, None, f"Error: {str(e)}"))

schema = T.StructType([
    T.StructField("table",             T.StringType()),
    T.StructField("num_files",         T.LongType()),
    T.StructField("size_gb",           T.DoubleType()),
    T.StructField("avg_file_mb",       T.DoubleType()),
    T.StructField("partitioned",       T.BooleanType()),
    T.StructField("liquid_clustering", T.BooleanType()),
    T.StructField("deletion_vectors",  T.BooleanType()),
    T.StructField("status",            T.StringType()),
])

display(
    spark.createDataFrame(results, schema=schema)
    .orderBy(F.col("avg_file_mb").asc_nulls_last())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
