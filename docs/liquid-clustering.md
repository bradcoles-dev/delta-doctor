# Liquid Clustering

> **Scope:** This guidance applies to Lakehouse (Spark/Delta) tables. Fabric Warehouse has its own clustering implementation, managed automatically behind the scenes — no action needed there. (Source: Miles Cole, Microsoft, Reddit r/MicrosoftFabric, 27 May 2026.)

## Partitioning is No Longer the Recommended Approach

Traditional Hive-style partitioning (e.g. `PARTITIONED BY (date)`) has effectively been superseded in Microsoft Fabric. Current documentation makes no mention of it as a recommended strategy — liquid clustering is the preferred approach for data organisation.

**Why partitioning is problematic in practice:**
- Creates a fixed physical directory structure that cannot be changed without rewriting all data
- Poor partition column choices (too many or too few distinct values) cause either too many small partitions or no meaningful skipping
- Does not adapt as query patterns change over time
- Conflicts with ATFS and auto-compaction, which work at the file level across the whole table

## What Liquid Clustering Is

Liquid clustering co-locates rows with similar values in the same files, enabling Delta's file-skipping to eliminate large portions of a table scan when queries filter on the cluster key columns. Unlike partitioning, the clustering policy is:

- **Changeable** — you can ALTER a table's clustering columns without rewriting all data
- **Flexible** — not tied to a fixed directory structure
- **Incremental** — applies to new files over time via OPTIMIZE; does not require a full rewrite upfront

## Critical Caveat: Clustering Only Applies When OPTIMIZE Runs

**Regular write operations do NOT physically cluster data.** Clustering is only applied when `OPTIMIZE` is explicitly run on the table.

This is the key operational implication:

> If you enable liquid clustering but never run `OPTIMIZE`, your data is not clustered — you get none of the file-skipping benefits.

This means liquid clustered tables **require** a compaction strategy. Options:

1. **Auto Compaction** — triggers OPTIMIZE inline after writes when fragmentation is detected; simplest approach for most pipelines
2. **Scheduled OPTIMIZE** — better for strict write-latency SLOs since auto compaction is synchronous; run on a separate Spark pool during quiet windows

## Cluster on Write

**Cluster on write is not a Fabric feature.** Microsoft Fabric's answer to write-time clustering is **incremental liquid clustering**, available from Runtime 2.0. Incremental clustering processes only unclustered, small, or deletion-vector-heavy files during OPTIMIZE — it does not cluster during the write itself, but makes OPTIMIZE cheap enough to run after every pipeline load. See the Runtime 2.0 section below.

Source: confirmed by Miles Cole (Principal PM, Microsoft), 2026-05-20. The Databricks-style cluster-on-write thresholds (e.g. 64 MB for Unity Catalog managed tables) do not apply to Fabric.

## How to Enable Liquid Clustering

**Using delta-optimizer:** Pass the `cluster_by` parameter to `dopt_utility_set_table_properties` as a comma-separated list of column names (e.g. `"customer_id, event_date"`). The notebook runs `ALTER TABLE ... CLUSTER BY (...)` and prints a reminder that clustering is applied physically on the next OPTIMIZE run. Leave `cluster_by` empty to skip — cluster key selection is a per-table decision and is not applied by `dopt_utility_set_properties_orchestrator`.

**Manually via SQL:** (`{table_path}` = `abfss://{workspace_guid}@onelake.dfs.fabric.microsoft.com/{lakehouse_guid}/Tables/{table_name}`)

```sql
-- On a new table
CREATE TABLE delta.`{table_path}`
(id INT, customer_id INT, event_date DATE, amount DECIMAL(10,2))
CLUSTER BY (customer_id, event_date);
```

```sql
-- On an existing table
ALTER TABLE delta.`{table_path}`
CLUSTER BY (customer_id, event_date);
```

Note: enabling on an existing table does not immediately recluster existing data. Run `OPTIMIZE` to apply clustering to existing files:

```sql
OPTIMIZE '{table_path}';
```

## When to Use Liquid Clustering

Liquid clustering is recommended for new Silver and Gold tables — but not every table benefits equally, and small static tables do not benefit at all.

**Use liquid clustering when:**
- The table is large enough to span multiple Parquet files
- Queries regularly filter on one or more high-cardinality columns (date ranges, customer IDs, product keys)
- Data distribution is skewed
- The table grows continuously and query patterns may evolve over time

**Skip liquid clustering when:**
- The table is a small static lookup or reference table (country codes, status types, currency codes) — if all data fits in one or two files, there is nothing to skip
- The table is append-only with no selective filtering in downstream queries

The platform's own automatic clustering heuristics reach the same conclusion: the Databricks documentation states that automatic liquid clustering will not select keys when "the table is too small to benefit." No hard threshold is published, but the underlying logic is straightforward — file-skipping only helps when there are multiple files to skip.

> **Note on Databricks documentation:** Databricks publishes write-size thresholds for when clustering-on-write triggers (e.g. 64 MB for a single-key Unity Catalog managed table). These figures apply to Databricks with Unity Catalog and are not directly applicable to Microsoft Fabric Lakehouses, which use a different catalogue model. The principle is the same — clustering on write does not trigger for trivially small transactions — but Fabric-specific thresholds are not published. When in doubt, verify against your own environment.

## Choosing Cluster Keys

Good cluster keys are columns that:
- Appear frequently in `WHERE` filters or join conditions
- Have reasonable cardinality (not too low like a boolean, not too high like a UUID)
- Reflect actual query patterns, not just ingestion patterns

Liquid clustering supports multiple columns. Unlike Z-Order, the column order matters less — the algorithm handles co-location across all specified keys.

## Liquid Clustering vs Z-Order

| | Liquid Clustering | Z-Order |
|---|---|---|
| Column order sensitivity | Lower | Higher |
| Changeability | ALTER TABLE, no full rewrite | Must rerun OPTIMIZE with new columns |
| Works with Fast Optimize | No (RT 1.3) / Yes (RT 2.0) | No — full OPTIMIZE always runs |
| Works with Auto Compaction | Yes — auto compaction triggers OPTIMIZE which applies clustering | Yes |
| Recommended for new tables | Yes | Legacy; prefer liquid clustering |

## Fast Optimize Does Not Apply (Runtime 1.3)

`spark.microsoft.delta.optimize.fast.enabled` has no effect on liquid clustered tables on Runtime 1.3 — OPTIMIZE always performs the full clustering pass. This is expected behaviour; Fast Optimize's bin-skipping logic is incompatible with the data movement required to enforce a clustering policy.

On Runtime 2.0, incremental clustering changes this: OPTIMIZE only processes unclustered, small, or degraded files — making it cheap enough that Fast Optimize's skip logic is less relevant.

## Native Execution Engine Integration (Mar 2026)

As of the March 2026 Fabric release, the Native Execution Engine has native support for both Z-Order and Liquid Clustering. This matters because clustering alone reduces I/O (file and row-group skipping), but pairing it with the Native Execution Engine adds vectorised, C++-based execution on top — the two optimisations compound rather than stack linearly.

**Why this matters for clustered tables:**
- Clustered data layout enables aggressive file skipping → fewer bytes scanned
- Native Execution Engine processes the remaining data through vectorised operators → fewer CPU cycles per byte
- Result: faster scans, lower compute cost, no query rewrites required

**Internal benchmarks (1B-row dataset, clustered columns):**
- 20–32 seconds absolute runtime reduction per query vs fallback execution
- ~20–27% improvement across multiple clustered column combinations
- Gains consistent across different predicate shapes and data distributions

**How to get these gains:**
1. Enable the Native Execution Engine at the workspace, environment, or session level
2. Apply Liquid Clustering or Z-Order using standard Delta commands (no changes needed)

Once both are active, supported Delta operations automatically use native execution paths — there is nothing else to configure.

## Runtime 2.0 Improvements

Current limitations in Fabric's liquid clustering implementation are addressed in Runtime 2.0. Miles Cole (Principal PM, Microsoft) fixed the core implementation logic, strengthening the recommendation for liquid clustering over partitioning from Runtime 2.0 onwards.

The dedicated Microsoft Learn page for Fabric liquid clustering is live: [Liquid Clustering — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/liquid-clustering). The accompanying RT 2.0 blog announcement landed 2026-05-27: [Incremental Liquid Clustering in Microsoft Fabric: Faster, smarter, and truly incremental](https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/Incremental-Liquid-Clustering-in-Microsoft-Fabric-Faster-smarter/ba-p/5189122) — Miles Cole, Principal PM, Microsoft. The post confirms incremental clustering targets unclustered/small/deletion-vector files for "constant-time" reclustering, auto-reclustering based on overlap thresholds, and that `OPTIMIZE ... FULL` remains available for full reclustering after a cluster-key change.

### Why the Old Behaviour Was So Costly: Z-Cube Sealing

In the comments on the announcement, Miles Cole explained the underlying mechanism — and clarified it isn't a Fabric-only quirk.

Standard Delta Lake liquid clustering (GA since Delta 3.2, and unchanged through 4.1) groups clustered files into **Z-Cubes**. Every time `OPTIMIZE` runs, any unclustered files plus existing files in a Z-Cube sharing the same clustering keys that together total under 100GB are rewritten with a new Z-Cube ID. This repeats — full rewrite, new Z-Cube ID — until the Z-Cube exceeds 100GB, at which point it is **sealed** and never reclustered again; a new Z-Cube starts for subsequent data.

The practical consequence: **any table under 100GB has its entire dataset rewritten every time OPTIMIZE finds new data to cluster.** It's technically incremental — just at 100GB granularity — but for the vast majority of real-world tables it behaves like a full rewrite on every run. That's the source of the severe write amplification when combined with Auto-Compaction on Runtime 1.3.

Fabric's Runtime 2.0 **Auto Reclustering** does not use this seal-at-100GB model. It identifies which specific files have degraded (overlapping clustering value ranges) and reclusters only those, regardless of total Z-Cube size. This is a genuine improvement over the current upstream OSS Delta algorithm for tables under 100GB — i.e. most tables — not merely a fix to a Fabric-specific bug.

Source: Miles Cole (Principal PM, Microsoft), replying to questions on the [Incremental Liquid Clustering announcement](https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/Incremental-Liquid-Clustering-in-Microsoft-Fabric-Faster-smarter/ba-p/5189122) on r/MicrosoftFabric, 27 May 2026.

## References

- [Table Compaction — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/table-compaction)
- [Liquid Clustering — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/liquid-clustering) — authored by Miles Cole (Principal PM, Microsoft); live as of 2026-05-20
- [Incremental Liquid Clustering in Microsoft Fabric: Faster, smarter, and truly incremental](https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/Incremental-Liquid-Clustering-in-Microsoft-Fabric-Faster-smarter/ba-p/5189122) — Miles Cole, Microsoft (27 May 2026), the RT 2.0 announcement
- [File Skipping for Delta Tables — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-file-skipping)
- [Delta Table Maintenance — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-table-maintenance)
- [Concurrency Control for Delta Tables — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-concurrency-control)
- [REORG for Delta Tables — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-reorg)
- [Microsoft Fabric Table Maintenance Optimization — Christopher Finlan (Feb 2026)](https://christopherfinlan.com/2026/02/15/microsoft-fabric-table-maintenance-optimization-a-cross-workload-survival-guide/)
- [Fabric March 2026 Feature Summary — Z-Order and Liquid Clustering in Native Execution Engine](https://blog.fabric.microsoft.com/en-us/blog/fabric-march-2026-feature-summary?ft=All#post-34196-_Toc224559614)
