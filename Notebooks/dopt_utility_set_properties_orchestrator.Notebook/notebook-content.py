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
# ## Purpose
# Iterates all tables in a Lakehouse and calls `dopt_utility_set_table_properties` for
# each, applying the correct Delta table properties for the given medallion layer. Run
# this once when onboarding a Lakehouse to delta-optimizer, or after adding a batch of
# new tables.
# ## What it does
# - Enumerates all tables via the OneLake ABFSS path — handles both schema-enabled and
#   non-schema Lakehouses automatically
# - Calls `dopt_utility_set_table_properties` for each table, passing the Lakehouse GUID,
#   table name, schema name, and layer
# - Catches and logs errors per table — one failing table does not stop the run
# - Prints a summary of tables updated and errored
# ## When to use this vs dopt_utility_set_table_properties
# Use this orchestrator to initialise an entire Lakehouse in one pipeline step. Once
# tables are configured, prefer calling `dopt_utility_set_table_properties` individually
# when adding new tables — there is no need to re-run the full Lakehouse on every change.
# ## One Lakehouse per layer
# This notebook assumes one Lakehouse per medallion layer, which is the standard Fabric
# pattern. Run it once per Lakehouse, passing the matching `layer` parameter each time:
# - Bronze Lakehouse → `layer = "bronze"`
# - Silver Lakehouse → `layer = "silver"`
# - Gold Lakehouse   → `layer = "gold"`
# ## Note on the workspace
# `mssparkutils.notebook.run()` identifies the target notebook by name within the same
# Fabric workspace. Ensure `dopt_utility_set_table_properties` has been imported into
# the same workspace as this orchestrator before running. Both notebooks must be in the
# same workspace as the target Lakehouse.


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

lakehouse_guid = ""        # The GUID of the Lakehouse to configure
layer          = "silver"  # Medallion layer: "bronze", "silver", or "gold". "custom" is not supported — use dopt_utility_set_table_properties directly for tables requiring non-standard configuration

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
# | `layer` | string | The medallion layer for all tables in this Lakehouse. Accepts `"bronze"`, `"silver"`, or `"gold"`. `"custom"` is not supported — use `dopt_utility_set_table_properties` directly for tables requiring non-standard configuration. Default: `"silver"` |


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

workspace_guid = mssparkutils.env.getWorkspaceId()

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

# ── Table enumeration ─────────────────────────────────────────────────────────

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
                    except:
                        pass
        except:
            pass
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Orchestration


# CELL ********************

# ── Orchestration ─────────────────────────────────────────────────────────────

tables = list_delta_tables(workspace_guid, lakehouse_guid)

updated_count = 0
error_count   = 0

print(f"Tables found: {len(tables)}")
print("-" * 60)

for entry in tables:
    schema_val   = entry["schema"]
    table_name   = entry["table"]
    display_name = f"{schema_val}.{table_name}" if schema_val else table_name
    try:
        mssparkutils.notebook.run(
            "dopt_utility_set_table_properties",
            timeout_seconds=120,
            arguments={
                "lakehouse_guid": lakehouse_guid,
                "table_name":     table_name,
                "schema_name":    schema_val,
                "layer":          layer,
            }
        )
        print(f"  {display_name}: properties set")
        updated_count += 1
    except Exception as e:
        print(f"  {display_name}: ERROR — {str(e)}")
        error_count += 1

print("-" * 60)
print(f"Summary — updated: {updated_count} | errors: {error_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
