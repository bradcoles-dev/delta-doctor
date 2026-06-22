# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # doctor_prevention_session_config
# ## Purpose
# Sets the Spark session configuration baseline for a given medallion layer.
# Call this notebook at the top of every pipeline notebook using
# `mssparkutils.notebook.run("doctor_prevention_session_config")`. It establishes a
# consistent, known configuration regardless of workspace defaults - which vary by workspace
# age and history. In an interactive notebook session, `%run doctor_prevention_session_config`
# also works — but `%run` is not available in Fabric pipeline Notebook activities.
# ## What it does
# - Applies the full session baseline (Auto-Compaction, ATFS, Fast Optimize, File Level
#   Compaction Target, Optimize Write, V-Order)
# - Applies layer-specific overrides on top of the baseline
# - Logs the active configuration for traceability in pipeline run logs
# ## Layer behaviour
# - **Bronze**: baseline + Optimize Write disabled (append-only loads do not benefit from shuffle)
# - **Silver**: baseline only
# - **Gold**: baseline + V-Order enabled (consumer-facing; Direct Lake and SQL Endpoint reads benefit)
# - **Custom**: baseline applied, then `custom_optimize_write` and `custom_v_order` parameter values
#   override the defaults. Use for architectures that do not follow Bronze / Silver / Gold.
# ## Custom mode
# Pass `layer = "custom"` for notebooks that do not follow a standard medallion layer.
# The full baseline is applied first, then `custom_optimize_write` and `custom_v_order`
# override the defaults explicitly. Both parameters must be `"true"` or `"false"` — the
# notebook raises a `ValueError` at startup if either is missing or invalid.
# ## Note on Optimize Write at Bronze
# The Bronze override disables Optimize Write for the common append-only batch ingestion case.
# If your Bronze ingestion pattern always uses MERGE, UPDATE, or DELETE, call this notebook
# with `layer = "custom"` and `custom_optimize_write = "true"` instead. Custom mode applies
# the full baseline and then explicitly enables Optimize Write — a permanent, documented
# configuration rather than a per-notebook override after the call.


# PARAMETERS CELL ********************

# ── Parameters ────────────────────────────────────────────────────────────────
# These values are overridden at runtime by the Fabric pipeline.
# Default values below are used when running the notebook interactively.

layer = "silver"    # Medallion layer: "bronze", "silver", "gold", or "custom"

# Custom mode parameters — only used when layer = "custom"
custom_optimize_write = "true"   # spark.databricks.delta.optimizeWrite.enabled: "true" or "false"
custom_v_order        = "false"  # spark.sql.parquet.vorder.default: "true" or "false"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters
# | Parameter | Type | Description |
# |---|---|---|
# | `layer` | string | Accepts `"bronze"`, `"silver"`, `"gold"`, or `"custom"`. Default: `"silver"` |
# | `custom_optimize_write` | string | **Custom mode only.** Sets `optimizeWrite.enabled`. Accepts `"true"` or `"false"`. Default: `"true"` |
# | `custom_v_order` | string | **Custom mode only.** Sets `vorder.default`. Accepts `"true"` or `"false"`. Default: `"false"` |


# MARKDOWN ********************

# ## Validation


# CELL ********************

# ── Validation ────────────────────────────────────────────────────────────────

valid_layers = {"bronze", "silver", "gold", "custom"}
valid_bool   = {"true", "false"}

if not layer or layer.lower() not in valid_layers:
    raise ValueError(f"Parameter 'layer' must be one of: {', '.join(sorted(valid_layers))}. Got: '{layer}'")

layer = layer.lower()

if layer == "custom":
    if str(custom_optimize_write).lower() not in valid_bool:
        raise ValueError(f"Parameter 'custom_optimize_write' must be 'true' or 'false'. Got: '{custom_optimize_write}'")
    if str(custom_v_order).lower() not in valid_bool:
        raise ValueError(f"Parameter 'custom_v_order' must be 'true' or 'false'. Got: '{custom_v_order}'")

print(f"Layer: {layer}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Baseline Configuration
# The following settings are applied to every session regardless of layer. They establish
# a consistent, known baseline - overriding workspace defaults that vary by workspace age
# and history.
# | Setting | Value | Why |
# |---|---|---|
# | `autoCompact.enabled` | `true` | Inline compaction after each write - prevents small file accumulation |
# | `targetFileSize.adaptive.enabled` | `true` | ATFS adjusts compaction target to table size - eliminates manual tuning |
# | `optimize.fast.enabled` | `true` | Skips OPTIMIZE on bins that don't need compaction - reduces write amplification |
# | `optimize.fileLevelTarget.enabled` | `true` | Prevents recompaction of already-optimised files when targets change |
# | `optimizeWrite.enabled` | `true` | Explicit baseline - overridden per layer below |
# | `vorder.default` | `false` | Explicit baseline - overridden per layer below |


# CELL ********************

# ── Baseline ──────────────────────────────────────────────────────────────────

spark.conf.set("spark.databricks.delta.autoCompact.enabled",            "true")
spark.conf.set("spark.microsoft.delta.targetFileSize.adaptive.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimize.fast.enabled",           "true")
spark.conf.set("spark.microsoft.delta.optimize.fileLevelTarget.enabled","true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",          "true")
spark.conf.set("spark.sql.parquet.vorder.default",                      "false")

print("Baseline configuration applied.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Layer Overrides
# Applied on top of the baseline. Only the settings that differ from the baseline are
# changed - everything else remains at the baseline value set above.


# CELL ********************

# ── Layer overrides ───────────────────────────────────────────────────────────

if layer == "bronze":
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
    print("Bronze override applied: Optimize Write disabled (append-only batch loads).")

elif layer == "gold":
    spark.conf.set("spark.sql.parquet.vorder.default", "true")
    print("Gold override applied: V-Order enabled (Direct Lake and SQL Endpoint consumers).")

elif layer == "custom":
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", str(custom_optimize_write).lower())
    spark.conf.set("spark.sql.parquet.vorder.default",             str(custom_v_order).lower())
    print(f"Custom override applied: Optimize Write = {custom_optimize_write}, V-Order = {custom_v_order}.")

else:
    print("Silver: no overrides - baseline is correct for this layer.")

print(f"\nSession configuration complete for layer: {layer}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
