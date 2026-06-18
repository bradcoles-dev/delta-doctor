# Deletion Vectors

## What They Are

By default, a `DELETE`, `UPDATE`, or `MERGE` that touches even a single row requires rewriting the entire Parquet file containing that row — expensive for large files.

Deletion vectors avoid this. Instead of rewriting the file, the operation records the affected rows as soft-deletes in a small sidecar file (the deletion vector). Subsequent reads resolve the current table state by applying the deletion vector on top of the original Parquet file. The file itself is only physically rewritten later, during compaction.

## When to Enable

Microsoft's official guidance by layer:

| Layer | Recommendation |
|---|---|
| Bronze | Enable for tables with merge patterns |
| Silver | Enable for tables with frequent updates |
| Gold | Enable; minimise accumulation through regular compaction (important for Direct Lake) |

## Why This Matters for Compaction

Soft-deletes are only physically applied when one of the following occurs:
- `OPTIMIZE` runs on the table
- Auto Compaction triggers a rewrite of a file that has a deletion vector
- `REORG TABLE ... APPLY (PURGE)` is run explicitly

If you have tables with frequent `DELETE`, `UPDATE`, or `MERGE` operations and no compaction strategy in place, soft-deletes accumulate. Reads become progressively slower as they resolve more and more deletion vectors on top of unchanged Parquet files.

**File sizing also matters:** with deletion vectors enabled, row-level tombstones in oversized files result in significant cleanup costs during compaction or purge operations. This is why Adaptive Target File Size and right-sized files are more important, not less, on tables with deletion vectors.

## Direct Lake Impact

Deletion vectors add overhead specifically to **Power BI Direct Lake** cold-state loading. During the bootstrapping phase (when a model loads data into memory from scratch), Direct Lake must load all deletion vectors for each table to correctly exclude soft-deleted rows from query results. The more deletion vectors accumulated, the higher this overhead.

The official recommendation for Direct Lake-consumed Gold tables: **minimise deletion vectors through regular compaction**. This is one of the reasons scheduled `OPTIMIZE` is still recommended at Gold even with auto-compaction enabled.

## VACUUM Implications

Deleted rows continue to physically exist in the original Parquet files until `VACUUM` removes them. The deleted data is just logically excluded via the deletion vector.

For tables with deletion vectors, the VACUUM flow is:
1. Run `OPTIMIZE` (or `REORG TABLE ... APPLY (PURGE)`) to physically rewrite files and resolve deletion vectors
2. Wait for the VACUUM retention window (minimum 7 days) from the time of the rewrite
3. Run `VACUUM` to remove the now-unreferenced original files

`REORG TABLE ... APPLY (PURGE)` is the more targeted option — it rewrites **only** files that contain deletion vectors, rather than running a full compaction pass:

```sql
REORG TABLE delta.`{table_path}` APPLY (PURGE);
```

After running this, note the completion timestamp — that is the reference point for your VACUUM retention window.

## How to Enable

**Using delta-optimizer:** Pass `lakehouse_guid`, `table_name`, and `layer` to `dopt_utility_set_table_properties`. Deletion vectors are enabled by default for all layers. Run `dopt_utility_set_properties_orchestrator` to enable across all tables in a Lakehouse at once.

**Manually via SQL:** (`{table_path}` = `abfss://{workspace_guid}@onelake.dfs.fabric.microsoft.com/{lakehouse_guid}/Tables/{table_name}`)

```sql
-- On a new table
CREATE TABLE delta.`{table_path}` (...)
TBLPROPERTIES ('delta.enableDeletionVectors' = true);

-- On an existing table
ALTER TABLE delta.`{table_path}`
SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

**Warning:** Enabling deletion vectors upgrades the Delta table protocol. The table will not be readable by clients that do not support deletion vectors. Verify client compatibility before enabling.

## Copy Activity Compatibility

The Lakehouse Copy Activity connector (Data Factory) supports deletion vectors for both source and destination:
- **Source:** Supported when the table uses reader version 3 with `deletionVectors` in `readerFeatures`. Soft-deleted rows are automatically skipped during reads.
- **Destination:** Supported for writes.

## Interaction with Fast Optimize

Fast Optimize's bin-skipping logic does not account for deletion vector overhead. A file that is technically large enough to skip compaction may still benefit from a rewrite if it carries a large deletion vector. `REORG TABLE ... APPLY (PURGE)` is the more appropriate tool for purging deletion vectors specifically.

## Summary

| Scenario | Recommended Action |
|---|---|
| Tables with frequent DELETEs/UPDATEs/MERGEs | Enable deletion vectors; ensure auto-compaction or scheduled OPTIMIZE is in place |
| Gold tables feeding Direct Lake | Run scheduled OPTIMIZE to minimise accumulated deletion vectors |
| Want to physically purge deleted rows now | `REORG TABLE ... APPLY (PURGE)`, then VACUUM after retention window |
| Full compaction + purge in one pass | `OPTIMIZE`, then VACUUM after retention window |
| Enabling on existing table | Test client compatibility first; protocol upgrade is not reversible on most table types |

## References

- [Cross-Workload Table Maintenance and Optimization — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/fundamentals/table-maintenance-optimization)
- [Understand Direct Lake Query Performance — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-understand-storage)
- [Tune File Size — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/tune-file-size)
- [Lakehouse Copy Activity — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-factory/connector-lakehouse-copy-activity)
- [Deletion Vectors — Azure Databricks (Microsoft Learn)](https://learn.microsoft.com/en-us/azure/databricks/delta/deletion-vectors)
- [optimize-vacuum.md](./optimize-vacuum.md)
