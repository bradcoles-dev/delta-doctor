# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # dopt_utility_set_table_properties
# ## Purpose
# Sets Delta table properties on a single table based on its medallion layer. These
# properties persist across Spark sessions and apply regardless of which notebook or
# pipeline writes the table — making them more reliable than session-level configs for
# tables with multiple writers.
# ## What it does
# - Applies the correct set of Delta table properties for the given layer in a single
#   `ALTER TABLE SET TBLPROPERTIES` call
# - Logs each property set and its value
# ## When to use this
# Run once per table at setup time, or call from an onboarding pipeline when a new table
# is added to the Lakehouse. It is not intended to run on every pipeline execution.
# ## Layer behaviour
# | Property | Bronze | Silver | Gold |
# |---|---|---|---|
# | `delta.enableDeletionVectors` | `true` | `true` | `true` |
# | `delta.autoOptimize.autoCompact` | `true` | `true` | `true` |
# | `delta.autoOptimize.optimizeWrite` | `false` | `true` | `true` |
# | `delta.parquet.vorder.enabled` | `false` | `false` | `true` |
# The logic mirrors `dopt_utility_session_config`:
# - **Bronze**: `optimizeWrite` is disabled — append-only batch loads do not benefit from
#   the shuffle that optimize write introduces
# - **Silver**: full baseline; V-Order is off because Silver tables are read by downstream
#   Spark notebooks, not directly by Power BI
# - **Gold**: V-Order enabled — consumer-facing tables served via Direct Lake or the SQL
#   Analytics Endpoint benefit from the write-time Parquet encoding
# ## Silver tables that feed Direct Lake directly
# If a Silver table is consumed directly by Power BI Direct Lake (skipping a Gold layer),
# call this notebook with `layer = "gold"` for that table. The Gold configuration is correct
# for any table that is the terminal layer for Direct Lake or SQL Endpoint consumers —
# regardless of what it is named.
# ## Warning — deletion vectors upgrade the table protocol
# Enabling deletion vectors upgrades the Delta table reader/writer protocol. The table will
# not be readable by clients that do not support deletion vectors. Verify client compatibility
# before enabling on a table that is read by external tools or connectors.


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

lakehouse_guid = ""        # The GUID of the Lakehouse containing the target table
table_name     = ""        # The table name (without schema prefix), e.g. "fact_sales"
layer          = "silver"  # Medallion layer: "bronze", "silver", or "gold"

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
# | `layer` | string | The medallion layer this table belongs to. Accepts `"bronze"`, `"silver"`, or `"gold"`. Default: `"silver"` |


# MARKDOWN ********************

# ## Validation


# CELL ********************

# ── Validation ────────────────────────────────────────────────────────────────

valid_layers = {"bronze", "silver", "gold"}

if not lakehouse_guid:
    raise ValueError("Parameter 'lakehouse_guid' is required but was not provided.")

if not table_name:
    raise ValueError("Parameter 'table_name' is required but was not provided.")

if not layer or layer.lower() not in valid_layers:
    raise ValueError(f"Parameter 'layer' must be one of: {', '.join(sorted(valid_layers))}. Got: '{layer}'")

layer                = layer.lower()
fully_qualified_name = f"{lakehouse_guid}.{table_name}"

print(f"Target table: {fully_qualified_name}")
print(f"Layer       : {layer}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set Table Properties
# Builds and executes the `ALTER TABLE SET TBLPROPERTIES` statement for the given layer.
# All four properties are set in a single DDL call.


# CELL ********************

# ── Layer definitions ─────────────────────────────────────────────────────────

LAYER_PROPERTIES = {
    "bronze": {
        "delta.enableDeletionVectors":       "true",
        "delta.autoOptimize.autoCompact":    "true",
        "delta.autoOptimize.optimizeWrite":  "false",
        "delta.parquet.vorder.enabled":      "false",
    },
    "silver": {
        "delta.enableDeletionVectors":       "true",
        "delta.autoOptimize.autoCompact":    "true",
        "delta.autoOptimize.optimizeWrite":  "true",
        "delta.parquet.vorder.enabled":      "false",
    },
    "gold": {
        "delta.enableDeletionVectors":       "true",
        "delta.autoOptimize.autoCompact":    "true",
        "delta.autoOptimize.optimizeWrite":  "true",
        "delta.parquet.vorder.enabled":      "true",
    },
}

props       = LAYER_PROPERTIES[layer]
props_str   = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())

spark.sql(f"ALTER TABLE {fully_qualified_name} SET TBLPROPERTIES ({props_str})")

print(f"Properties set on {fully_qualified_name}:")
for k, v in props.items():
    print(f"  {k} = {v}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
