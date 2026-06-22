# Spark Session Config Utility

Reference for the settings applied by `doctor_prevention_session_config`. Covers what each setting does, the Fabric default, and when to override.

## Current Config

```python
# Enable Auto-compaction — compacts small files automatically after writes
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Enable Adaptive Target File Size — adapts compaction target based on table size
spark.conf.set("spark.microsoft.delta.targetFileSize.adaptive.enabled", "true")

# Enable Fast Optimize — skips OPTIMIZE when files don't genuinely need compaction (manual OPTIMIZE only)
spark.conf.set("spark.microsoft.delta.optimize.fast.enabled", "true")

# Enable File Level Compaction Target — prevents recompaction of already-optimised files when target sizes change
spark.conf.set("spark.microsoft.delta.optimize.fileLevelTarget.enabled", "true")

# Optimize Write: set explicitly — workspace default varies with age and history; this establishes a known baseline
# Override to false in append-only batch notebooks where shuffle overhead is not justified
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# V-Order: set explicitly to off — workspace default varies with age and history; this establishes a known baseline
# Override to true at session level in Gold notebooks, or per-table via TBLPROPERTIES at Silver
spark.conf.set("spark.sql.parquet.vorder.default", "false")
```

## Setting Reference

Fabric defaults differ from Azure Synapse Analytics in two important ways — see the [official docs](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables).

| Setting | Fabric Default | Effect | Caveat |
|---|---|---|---|
| `spark.databricks.delta.autoCompact.enabled` | `false` | After each commit, Fabric compacts small files into larger ones inline | Universally recommended. Minor post-write latency; saves significant long-term fragmentation |
| `spark.microsoft.delta.targetFileSize.adaptive.enabled` | `false` | ATFS adapts the compaction target based on table size — scaling the target downward for small tables and converging on the configured ceiling for large ones | Best paired with `autoCompact`. Makes manual `OPTIMIZE` largely redundant for ongoing loads |
| `spark.databricks.delta.optimizeWrite.enabled` | **`true`** | Reshuffles data at write time to produce fewer, larger files matching the target size | **On by default in Fabric** — explicitly disable in batch ETL notebooks; see note below |
| `spark.microsoft.delta.optimize.fast.enabled` | `false` | Skips `OPTIMIZE` when files don't genuinely need compaction — 80% reduction in compaction time reported | Applies to manual `OPTIMIZE` only; Auto Compaction uses its own logic |
| `spark.microsoft.delta.optimize.fileLevelTarget.enabled` | `false` | Tags compacted files with the target size used, preventing recompaction when target sizes change | Pairs well with ATFS; reduces write amplification over time |
| `spark.sql.parquet.vorder.default` | `false` | V-Order encoding for Power BI Direct Lake and SQL Endpoint read performance | See [v-order.md](./v-order.md) |

## Optimize Write — When to Enable vs Disable

**Optimize Write is on by default in Fabric** (unlike Azure Synapse Analytics where it was unset). It reshuffles in-memory data into optimally sized bins before writing Parquet files — reducing downstream compaction pressure at the cost of shuffle overhead.

Since tables use Liquid Clustering (not partitioning), the decision is straightforward:

| Notebook type | Action | Reason |
|---|---|---|
| `MERGE`, `UPDATE`, or `DELETE` | Leave at default (`true`) | These operations touch many files; pre-write bin packing reduces compaction pressure |
| Intraday / streaming small writes | Leave at default (`true`) | Produces small files that benefit from bin packing |
| Append-only batch loads | **Disable (`false`)** | Large writes already produce right-sized files; shuffle overhead adds cost with no benefit |

```python
# Add to append-only batch notebooks after calling the session config notebook
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
```

Since MERGE notebooks exist at every layer, the Fabric default (`true`) is the right starting point. Append-only batch notebooks are the exception that explicitly opt out.

## References

- [Lakehouse and Delta Tables — Microsoft Learn (official)](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables)
- [Announcing Optimized Compaction in Fabric Spark — Microsoft Fabric Blog, Miles Cole (Oct 2025)](https://blog.fabric.microsoft.com/en-us/blog/announcing-optimized-compaction-in-fabric-spark)
- [Microsoft Fabric Table Maintenance Optimization — Christopher Finlan (Feb 2026)](https://christopherfinlan.com/2026/02/15/microsoft-fabric-table-maintenance-optimization-a-cross-workload-survival-guide/)
