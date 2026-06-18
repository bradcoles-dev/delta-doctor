# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # dopt_utility_set_properties_orchestrator
# # ## Purpose
# Iterates all tables in a Lakehouse and calls `dopt_utility_set_table_properties` for
# each, applying the correct Delta table properties for the given medallion layer. Run
# this once when onboarding a Lakehouse to delta-optimizer, or after adding a batch of
# new tables.
# # ## What it does
# - Lists all tables in the Lakehouse via `SHOW TABLES`
# - Calls `dopt_utility_set_table_properties` for each table, passing the Lakehouse GUID,
#   table name, and layer
# - Catches and logs errors per table — one failing table does not stop the run
# - Prints a summary of tables updated and errored
# # ## When to use this vs dopt_utility_set_table_properties
# Use this orchestrator to initialise an entire Lakehouse in one pipeline step. Once
# tables are configured, prefer calling `dopt_utility_set_table_properties` individually
# when adding new tables — there is no need to re-run the full Lakehouse on every change.
# # ## One Lakehouse per layer
# This notebook assumes one Lakehouse per medallion layer, which is the standard Fabric
# pattern. Run it once per Lakehouse, passing the matching `layer` parameter each time:
# - Bronze Lakehouse → `layer = "bronze"`
# - Silver Lakehouse → `layer = "silver"`
# - Gold Lakehouse   → `layer = "gold"`
# # ## Note on the workspace name
# `mssparkutils.notebook.run()` identifies the target notebook by name within the same
# Fabric workspace. Ensure `dopt_utility_set_table_properties` has been imported into
# the same workspace as this orchestrator before running.


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

lakehouse_guid = ""        # The GUID of the Lakehouse to configure
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
# | `lakehouse_guid` | string | The GUID of the Lakehouse to configure. Found in the Lakehouse URL in the Fabric portal |
# | `layer` | string | The medallion layer for all tables in this Lakehouse. Accepts `"bronze"`, `"silver"`, or `"gold"`. Default: `"silver"` |


# MARKDOWN ********************

# ## Validation


# CELL ********************

# ── Validation ────────────────────────────────────────────────────────────────

valid_layers = {"bronze", "silver", "gold"}

if not lakehouse_guid:
    raise ValueError("Parameter 'lakehouse_guid' is required but was not provided.")

if not layer or layer.lower() not in valid_layers:
    raise ValueError(f"Parameter 'layer' must be one of: {', '.join(sorted(valid_layers))}. Got: '{layer}'")

layer = layer.lower()

print(f"Lakehouse: {lakehouse_guid}")
print(f"Layer    : {layer}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Orchestration
# Iterates all tables in the Lakehouse and calls `dopt_utility_set_table_properties` for
# each. Errors on individual tables are caught and logged — the run continues regardless.


# CELL ********************

# ── Orchestration ─────────────────────────────────────────────────────────────

tables = spark.sql(f"SHOW TABLES IN {lakehouse_guid}").collect()

updated_count = 0
error_count   = 0

print(f"Tables found: {len(tables)}")
print("-" * 60)

for row in tables:
    table_name = row.tableName
    try:
        mssparkutils.notebook.run(
            "dopt_utility_set_table_properties",
            timeout_seconds=120,
            arguments={
                "lakehouse_guid": lakehouse_guid,
                "table_name":     table_name,
                "layer":          layer,
            }
        )
        print(f"  {table_name}: properties set")
        updated_count += 1
    except Exception as e:
        print(f"  {table_name}: ERROR - {str(e)}")
        error_count += 1

print("-" * 60)
print(f"Summary - updated: {updated_count} | errors: {error_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
