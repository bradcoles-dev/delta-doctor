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
# # ## Purpose
# Sets Delta table properties on a single table. These properties persist across Spark
# sessions and apply regardless of which notebook or pipeline writes the table — making
# them more reliable than session-level configs for tables with multiple writers.
# # ## What it does
# - Sets any combination of the five configurable table properties
# - Skips any property where the parameter is left empty (or zero for file size)
# - Prints a clear log of every property set and its value
# # ## When to use this
# - **Deletion vectors**: enable on any table with frequent MERGE, UPDATE, or DELETE
# - **Auto-Compaction / Optimize Write at table level**: use instead of session configs
#   when multiple notebooks or pipelines write to the same table
# - **V-Order**: enable on individual Silver tables that feed Power BI Direct Lake or
#   the SQL Analytics Endpoint
# - **Target file size**: override ATFS for a specific table (e.g. lock a Gold table
#   to 400 MB regardless of how large the table grows)
# # ## Warning — deletion vectors upgrade the table protocol
# Enabling deletion vectors upgrades the Delta table reader/writer protocol. The table
# will not be readable by clients that do not support deletion vectors. Verify client
# compatibility before enabling on a table that is read by external tools or connectors.


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.
#
# For string parameters: pass "true" or "false" to set the property.
# Leave as "" to skip that property — it will not be changed.
# For target_file_size_mb: pass a positive integer to set the property.
# Leave as 0 to skip.

lakehouse_guid      = ""       # The GUID of the Lakehouse containing the target table
table_name          = ""       # The table name (without schema prefix), e.g. "fact_sales"
deletion_vectors    = "true"   # delta.enableDeletionVectors     — "true", "false", or "" to skip
auto_compact        = "true"   # delta.autoOptimize.autoCompact  — "true", "false", or "" to skip
optimize_write      = ""       # delta.autoOptimize.optimizeWrite — "true", "false", or "" to skip
v_order             = ""       # delta.parquet.vorder.enabled    — "true", "false", or "" to skip
target_file_size_mb = 0        # delta.targetFileSize            — positive integer (MB), or 0 to skip

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# # | Parameter | Type | Default | Description |
# |---|---|---|---|
# | `lakehouse_guid` | string | — | The GUID of the Lakehouse. Found in the Lakehouse URL in the Fabric portal |
# | `table_name` | string | — | Table name without schema prefix (e.g. `fact_sales`) |
# | `deletion_vectors` | string | `"true"` | Sets `delta.enableDeletionVectors`. Pass `""` to skip |
# | `auto_compact` | string | `"true"` | Sets `delta.autoOptimize.autoCompact`. Pass `""` to skip |
# | `optimize_write` | string | `""` | Sets `delta.autoOptimize.optimizeWrite`. Pass `""` to skip |
# | `v_order` | string | `""` | Sets `delta.parquet.vorder.enabled`. Pass `""` to skip |
# | `target_file_size_mb` | integer | `0` | Sets `delta.targetFileSize` in MB (e.g. `400`). Pass `0` to skip |
# # ### Why empty string means skip
# Fabric pipeline parameters are always passed — there is no way to omit one entirely.
# Empty string is used as a sentinel value meaning "do not change this property."
# This allows the notebook to be called with only the properties you intend to change,
# leaving everything else untouched.
# # ### Default values
# `deletion_vectors` and `auto_compact` default to `"true"` because they are safe to
# enable on almost all tables and are the most common reason to call this notebook.
# `optimize_write`, `v_order`, and `target_file_size_mb` default to skip because their
# correct values depend on table type and layer — pass them explicitly when needed.


# MARKDOWN ********************

# ## Validation


# CELL ********************

# ── Validation ────────────────────────────────────────────────────────────────

if not lakehouse_guid:
    raise ValueError("Parameter 'lakehouse_guid' is required but was not provided.")

if not table_name:
    raise ValueError("Parameter 'table_name' is required but was not provided.")

valid_values = {"true", "false", ""}

for param_name, param_value in [
    ("deletion_vectors", deletion_vectors),
    ("auto_compact",     auto_compact),
    ("optimize_write",   optimize_write),
    ("v_order",          v_order),
]:
    if param_value.lower() not in valid_values:
        raise ValueError(f"Parameter '{param_name}' must be 'true', 'false', or '' to skip. Got: '{param_value}'")

if target_file_size_mb < 0:
    raise ValueError(f"Parameter 'target_file_size_mb' must be 0 (skip) or a positive integer. Got: {target_file_size_mb}")

fully_qualified_name = f"{lakehouse_guid}.{table_name}"
print(f"Target table: {fully_qualified_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set Table Properties
# # Builds the `ALTER TABLE SET TBLPROPERTIES` statement from the parameters provided.
# Properties with empty string or zero values are skipped entirely — the table's
# existing value for those properties is left unchanged.


# CELL ********************

# ── Set table properties ───────────────────────────────────────────────────────

props_to_set = {}

if deletion_vectors != "":
    props_to_set["delta.enableDeletionVectors"] = deletion_vectors.lower()

if auto_compact != "":
    props_to_set["delta.autoOptimize.autoCompact"] = auto_compact.lower()

if optimize_write != "":
    props_to_set["delta.autoOptimize.optimizeWrite"] = optimize_write.lower()

if v_order != "":
    props_to_set["delta.parquet.vorder.enabled"] = v_order.lower()

if target_file_size_mb > 0:
    props_to_set["delta.targetFileSize"] = f"{target_file_size_mb}m"

if not props_to_set:
    print("No properties to set — all parameters were empty or zero. Nothing was changed.")
else:
    props_str = ", ".join(f"'{k}' = '{v}'" for k, v in props_to_set.items())
    spark.sql(f"ALTER TABLE {fully_qualified_name} SET TBLPROPERTIES ({props_str})")

    print(f"Properties set on {fully_qualified_name}:")
    for k, v in props_to_set.items():
        print(f"  {k} = {v}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
