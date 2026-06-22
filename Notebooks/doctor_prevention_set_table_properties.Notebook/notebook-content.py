# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # doctor_prevention_set_table_properties
# ## Purpose
# Sets Delta table properties on a single table based on its medallion layer. These
# properties persist across Spark sessions and apply regardless of which notebook or
# pipeline writes the table — making them more reliable than session-level configs for
# tables with multiple writers.
# ## Warning — deletion vectors upgrade the table protocol
# Enabling deletion vectors upgrades the Delta table reader/writer protocol — this is
# **not reversible** on most table types without rewriting the table. The table will not be
# readable by clients that do not support deletion vectors, including Azure Synapse Analytics
# external tables, some third-party connectors, and Databricks runtimes prior to 11.3 LTS.
# Verify external reader compatibility before running. To apply properties selectively to
# individual tables, use this notebook directly rather than the orchestrator.
# ## What it does
# - Applies the correct set of Delta table properties for the given layer in a single
#   `ALTER TABLE SET TBLPROPERTIES` call
# - Logs each property set and its value
# ## When to use this
# Run once per table at setup time, or call from an onboarding pipeline when a new table
# is added to the Lakehouse. It is not intended to run on every pipeline execution.
# ## Custom mode
# Pass `layer = "custom"` to specify each property value explicitly. In custom mode the
# layer defaults are ignored and only the properties you specify are set. Leave a custom
# parameter as `""` (or `0` for `target_file_size_mb`) to skip that property entirely.
# Use custom mode for tables that do not fit a standard medallion layer — data products,
# external-facing tables, or architectures that do not follow Bronze / Silver / Gold.
# ## Layer behaviour
# | Property | Bronze | Silver | Gold |
# |---|---|---|---|
# | `delta.enableDeletionVectors` | `true` | `true` | `true` |
# | `delta.autoOptimize.autoCompact` | `true` | `true` | `true` |
# | `delta.autoOptimize.optimizeWrite` | `false` | `true` | `true` |
# | `delta.parquet.vorder.enabled` | `false` | `false` | `true` |
# | `delta.targetFileSize` | 128 MB | 256 MB | 400 MB |
# ### delta.targetFileSize and ATFS
# `delta.targetFileSize` sets a per-table ceiling. Adaptive Target File Size (ATFS),
# enabled in `doctor_prevention_session_config`, adapts that target downward for small tables —
# preventing a 10 MB table from being compacted into a single 400 MB file. The two settings
# work together: ATFS needs a ceiling to adapt from; this property provides it per table
# rather than relying on a single workspace-wide default.
# The logic mirrors `doctor_prevention_session_config`:
# - **Bronze**: `optimizeWrite` is disabled — append-only batch loads do not benefit from
#   the shuffle that optimize write introduces. For Bronze MERGE/UPDATE/DELETE pipelines,
#   session config takes precedence over table properties for this setting — override it
#   in the pipeline notebook: `spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")`
#   or call `doctor_prevention_session_config` with `layer = "custom"` and
#   `custom_optimize_write = True`
# - **Silver**: full baseline; V-Order is off because Silver tables are read by downstream
#   Spark notebooks, not directly by Power BI
# - **Gold**: V-Order enabled — consumer-facing tables served via Direct Lake or the SQL
#   Analytics Endpoint benefit from the write-time Parquet encoding
# ## Silver tables that feed Direct Lake directly
# If a Silver table is consumed directly by Power BI Direct Lake (skipping a Gold layer),
# call this notebook with `layer = "gold"` for that table. The Gold configuration is correct
# for any table that is the terminal layer for Direct Lake or SQL Endpoint consumers —
# regardless of what it is named.
# ## Liquid clustering
# Pass a comma-separated list of column names in `cluster_by` to enable liquid clustering
# on the table (e.g. `"customer_id, order_date"`). Leave empty to skip.
# Liquid clustering replaces partitioning — do not enable it on a table that already uses
# traditional `PARTITION BY`. Check the `partitioned` column in `doctor_diagnosis_table_health`
# before enabling.
# Enabling clustering does not physically cluster the data. The next OPTIMIZE run (via
# `doctor_treatment_table_maintenance` or the orchestrator) applies it.


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

lakehouse_guid = ""        # The GUID of the Lakehouse containing the target table
table_name     = ""        # The table name (without schema prefix), e.g. "fact_sales"
schema_name    = ""        # Schema name for schema-enabled Lakehouses. Leave empty for non-schema Lakehouses
layer          = "silver"  # Medallion layer: "bronze", "silver", "gold", or "custom"
cluster_by     = ""        # Comma-separated cluster key columns, e.g. "customer_id, order_date". "" to skip

# Custom mode parameters — only used when layer = "custom". "" to skip a property.
custom_deletion_vectors   = ""   # delta.enableDeletionVectors: "true", "false", or "" to skip
custom_auto_compact       = ""   # delta.autoOptimize.autoCompact: "true", "false", or "" to skip
custom_optimize_write     = ""   # delta.autoOptimize.optimizeWrite: "true", "false", or "" to skip
custom_v_order            = ""   # delta.parquet.vorder.enabled: "true", "false", or "" to skip
custom_target_file_size_mb = 0       # delta.targetFileSize in MB; 0 to skip

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# | Parameter | Type | Description |
# |---|---|---|
# | `lakehouse_guid` | string | The GUID of the Lakehouse. Found in the Lakehouse URL in the Fabric portal |
# | `table_name` | string | Table name without schema prefix (e.g. `fact_sales`) |
# | `schema_name` | string | Schema name for schema-enabled Lakehouses (e.g. `silver`). Leave empty for Lakehouses without schemas |
# | `layer` | string | Accepts `"bronze"`, `"silver"`, `"gold"`, or `"custom"`. Default: `"silver"` |
# | `cluster_by` | string | Comma-separated column names for liquid clustering (e.g. `"customer_id, order_date"`). `""` to skip. Do not use on partitioned tables |
# | `custom_deletion_vectors` | string | **Custom mode only.** `"true"`, `"false"`, or `""` to skip |
# | `custom_auto_compact` | string | **Custom mode only.** `"true"`, `"false"`, or `""` to skip |
# | `custom_optimize_write` | string | **Custom mode only.** `"true"`, `"false"`, or `""` to skip |
# | `custom_v_order` | string | **Custom mode only.** `"true"`, `"false"`, or `""` to skip |
# | `custom_target_file_size_mb` | integer | **Custom mode only.** Target file size in MB. `0` to skip |


# MARKDOWN ********************

# ## Validation


# CELL ********************

# ── Validation ────────────────────────────────────────────────────────────────

valid_layers      = {"bronze", "silver", "gold", "custom"}
valid_bool_values = {"true", "false", ""}

if not lakehouse_guid:
    raise ValueError("Parameter 'lakehouse_guid' is required but was not provided.")

if not table_name:
    raise ValueError("Parameter 'table_name' is required but was not provided.")

if not layer or layer.lower() not in valid_layers:
    raise ValueError(f"Parameter 'layer' must be one of: {', '.join(sorted(valid_layers))}. Got: '{layer}'")

layer = layer.lower()

if layer == "custom":
    for param_name, param_value in [
        ("custom_deletion_vectors", custom_deletion_vectors),
        ("custom_auto_compact",     custom_auto_compact),
        ("custom_optimize_write",   custom_optimize_write),
        ("custom_v_order",          custom_v_order),
    ]:
        if param_value.lower() not in valid_bool_values:
            raise ValueError(f"Parameter '{param_name}' must be 'true', 'false', or '' to skip. Got: '{param_value}'")
    if custom_target_file_size_mb < 0:
        raise ValueError(f"Parameter 'custom_target_file_size_mb' must be 0 (skip) or a positive integer. Got: {custom_target_file_size_mb}")

workspace_guid = mssparkutils.env.getWorkspaceId()
onelake_base   = f"abfss://{workspace_guid}@onelake.dfs.fabric.microsoft.com/{lakehouse_guid}/Tables"
table_path     = f"{onelake_base}/{schema_name}/{table_name}" if schema_name else f"{onelake_base}/{table_name}"
display_name   = f"{schema_name}.{table_name}" if schema_name else table_name

print(f"Target table: {display_name}")
print(f"Layer       : {layer}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set Table Properties
# Builds and executes the `ALTER TABLE SET TBLPROPERTIES` statement for the given layer.
# If `cluster_by` is provided, the table's partition columns are checked **before** any
# modifications are applied — liquid clustering and traditional partitioning cannot be
# combined, and this guard ensures no DDL runs on an invalid configuration. Properties
# are then set in a single DDL call, followed by `ALTER TABLE CLUSTER BY` if clustering
# was requested.


# CELL ********************

# ── Layer definitions ─────────────────────────────────────────────────────────

LAYER_PROPERTIES = {
    "bronze": {
        "delta.enableDeletionVectors":       "true",
        "delta.autoOptimize.autoCompact":    "true",
        "delta.autoOptimize.optimizeWrite":  "false",
        "delta.parquet.vorder.enabled":      "false",
        "delta.targetFileSize":              str(128 * 1024 * 1024),   # 128 MB in bytes
    },
    "silver": {
        "delta.enableDeletionVectors":       "true",
        "delta.autoOptimize.autoCompact":    "true",
        "delta.autoOptimize.optimizeWrite":  "true",
        "delta.parquet.vorder.enabled":      "false",
        "delta.targetFileSize":              str(256 * 1024 * 1024),   # 256 MB in bytes
    },
    "gold": {
        "delta.enableDeletionVectors":       "true",
        "delta.autoOptimize.autoCompact":    "true",
        "delta.autoOptimize.optimizeWrite":  "true",
        "delta.parquet.vorder.enabled":      "true",
        "delta.targetFileSize":              str(400 * 1024 * 1024),   # 400 MB in bytes
    },
}

if layer == "custom":
    props = {}
    if custom_deletion_vectors.strip():
        props["delta.enableDeletionVectors"]      = custom_deletion_vectors.lower()
    if custom_auto_compact.strip():
        props["delta.autoOptimize.autoCompact"]   = custom_auto_compact.lower()
    if custom_optimize_write.strip():
        props["delta.autoOptimize.optimizeWrite"] = custom_optimize_write.lower()
    if custom_v_order.strip():
        props["delta.parquet.vorder.enabled"]     = custom_v_order.lower()
    if custom_target_file_size_mb > 0:
        props["delta.targetFileSize"]             = str(custom_target_file_size_mb * 1024 * 1024)
    if not props:
        print("Custom mode: no properties specified — nothing was changed.")
else:
    props = LAYER_PROPERTIES[layer]

props_str = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())

if cluster_by.strip():
    detail = spark.sql(f"DESCRIBE DETAIL '{table_path}'").first()
    if detail.partitionColumns:
        raise ValueError(
            f"{display_name} is partitioned on {list(detail.partitionColumns)}. "
            "Liquid clustering and traditional partitioning cannot be combined. "
            "To migrate: rewrite the table without PARTITION BY, then re-run with cluster_by."
        )

if props:
    spark.sql(f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES ({props_str})")
    print(f"Properties set on {display_name}:")
    for k, v in props.items():
        print(f"  {k} = {v}")

if cluster_by.strip():
    spark.sql(f"ALTER TABLE delta.`{table_path}` CLUSTER BY ({cluster_by.strip()})")
    print(f"  liquid clustering enabled on: {cluster_by.strip()}")
    print("  Note: clustering is applied physically on the next OPTIMIZE run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
