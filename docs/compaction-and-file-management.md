# Compaction and File Management

## The Small Files Problem

Each Spark write (especially streaming, intraday, or partitioned batch loads) produces multiple small Parquet files. Small files hurt query performance because:
- The SQL Analytics Endpoint and Power BI must open many files per query
- Delta transaction log grows with each file entry
- Metadata operations (listing, checkpointing) become slower over time

The traditional solution was a scheduled `OPTIMIZE` job. With the settings below, this is largely handled automatically.

## Auto-Compaction

**Setting:** `spark.databricks.delta.autoCompact.enabled = true`

After each write commits, Fabric checks whether the files in the affected table partitions are below the target file size and, if so, runs an inline compaction pass.

- No separate job or schedule required
- Compaction happens in the same Spark session as the write — it runs **synchronously** after the write commits
- Adds write latency as a result; for pipelines with strict latency SLOs, a scheduled `OPTIMIZE` on a separate Spark pool may be preferable
- For overnight batch loads where write latency is not a concern, the synchronous overhead is acceptable
- **Recommended for most ingestion workloads** — Microsoft's default recommendation

## Adaptive Target File Size (ATFS)

**Setting:** `spark.microsoft.delta.targetFileSize.adaptive.enabled = true`

Rather than targeting a fixed file size (the default is 128 MB), ATFS adapts the compaction target based on table size — preventing a 10 MB table from being forced into a single 400 MB file. For small tables it scales the target downward proportionally; for large tables it converges on the configured ceiling.

`doctor_prevention_set_table_properties` sets `delta.targetFileSize` as a per-table ceiling. ATFS then adapts downward from that ceiling for small tables. The two work together: ATFS needs a ceiling to adapt from; the table property provides it.

### ATFS + Auto-Compaction Together

When both are enabled, auto-compaction uses the ATFS-calculated target. The result is that both the inline compaction after writes and any explicit `OPTIMIZE` runs converge on the same optimal file size. This is the recommended combination.

> "With ATFS enabled, both operations converge on the same target, making separate scheduling redundant for most workloads."
> — Christopher Finlan, Feb 2026

## Fast Optimize

**Setting:** `spark.microsoft.delta.optimize.fast.enabled = true`

Rather than blindly compacting whenever small files exist, Fast Optimize evaluates whether the files in a table genuinely need compaction before doing any work. It skips the operation entirely when the benefit would be negligible — measured against a minimum file size and minimum file count threshold.

- Applies to explicit `OPTIMIZE` runs only — **Auto Compaction uses its own internal logic and is unaffected**
- **Not applicable to Liquid Clustering or Z-Order operations** — OPTIMIZE always runs the full pass on liquid clustered tables
- Reported 80% reduction in time spent on compaction over 200 ELT cycles with no performance regression (Microsoft, Oct 2025)
- No known downside — keep enabled

## File Level Compaction Target

**Setting:** `spark.microsoft.delta.optimize.fileLevelTarget.enabled = true`

When files are compacted, this stores the target file size used as metadata alongside the file. Future `OPTIMIZE` or auto-compaction runs treat those files as already-compacted if their size is at least half of the stored target — preventing unnecessary recompaction if target sizes shift over time.

This pairs well with ATFS: as ATFS adapts the target size, File Level Target prevents files that were compacted to a previous target from being needlessly rewritten.

- Recommended alongside ATFS and Fast Optimize
- Reduces write amplification when optimization strategy evolves

## Practical Implication: No Nightly OPTIMIZE Required

For **Bronze append-only** tables with active ongoing writes, auto-compaction + ATFS handles file sizing continuously. A scheduled nightly `OPTIMIZE` job is **not needed** for routine file management.

**Silver and Gold are different.** Auto-compaction targets 128 MB by default; Silver and Gold consumers (SQL Analytics Endpoint, Power BI Direct Lake) need 256–400 MB files. Liquid Clustering only applies physically when OPTIMIZE runs. Deletion vectors accumulate and must be resolved. For Silver and Gold, scheduled OPTIMIZE is still recommended — see [optimize-vacuum.md](./optimize-vacuum.md) for guidance.

## Fabric Defaults Note

`autoCompact` and ATFS are **not** on by default in Fabric — both must be explicitly enabled. This is consistent with what the utility notebook sets. See the [official docs](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables) for the full Fabric default configuration table.

## When You Still Need Explicit OPTIMIZE

Situations where you still need an explicit `OPTIMIZE`:
- After a large historical backfill or initial data load
- After schema evolution that rewrites many files
- To force V-Order re-encoding on existing files (see [v-order.md](./v-order.md))
- On tables with very infrequent writes where auto-compaction never triggers at meaningful scale

See [optimize-vacuum.md](./optimize-vacuum.md) for those cases.

## References

- [Table Compaction — Microsoft Learn (official)](https://learn.microsoft.com/en-us/fabric/data-engineering/table-compaction)
- [Lakehouse and Delta Tables — Microsoft Learn (official)](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables)
- [Announcing Optimized Compaction in Fabric Spark — Microsoft Fabric Blog, Miles Cole (Oct 2025)](https://blog.fabric.microsoft.com/en-us/blog/announcing-optimized-compaction-in-fabric-spark)
- [Microsoft Fabric Table Maintenance Optimization — Christopher Finlan (Feb 2026)](https://christopherfinlan.com/2026/02/15/microsoft-fabric-table-maintenance-optimization-a-cross-workload-survival-guide/)
