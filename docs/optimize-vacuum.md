# OPTIMIZE and VACUUM

## OPTIMIZE

### When Auto-Compaction Is Sufficient

For Bronze-layer append-only ingestion workloads, auto-compaction + ATFS handles file sizing continuously and a scheduled OPTIMIZE is not needed for routine file management.

### When Scheduled OPTIMIZE Is Recommended

The official Microsoft guidance is more nuanced than "auto-compaction handles everything." For Silver and Gold tables — especially those serving the SQL Analytics Endpoint or Power BI Direct Lake — **scheduled OPTIMIZE should be run aggressively**.

Reasons:
- Auto-compaction's default target file size is 128 MB; SQL Analytics Endpoint and Direct Lake prefer 400 MB–1 GB files
- Direct Lake performance degrades as deletion vectors accumulate — regular OPTIMIZE physically resolves them
- Liquid Clustering only takes effect when OPTIMIZE runs; writes alone don't cluster data
- Auto-compaction runs synchronously and may not fully compact across all partitions in one pass

| Layer | Recommended OPTIMIZE Strategy |
|---|---|
| Bronze (append-only) | Not needed — auto-compaction sufficient |
| Bronze (with MERGE patterns) | Run after MERGE-heavy loads |
| Silver | Run aggressively — after loads or on a schedule |
| Gold | Run aggressively — especially before Direct Lake framing windows |

### Additional Scenarios Requiring Explicit OPTIMIZE

| Scenario | Reason |
|---|---|
| After a large historical backfill or initial load | Auto-compaction only triggers on the write session; bulk loads leave many fragmented files |
| After schema evolution | File layout may not reflect ATFS targets until recompacted |
| To apply V-Order to existing files | V-Order only encodes newly-written files; OPTIMIZE forces re-encoding |
| Tables with Liquid Clustering | Data is only physically clustered when OPTIMIZE runs |
| Tables with accumulated deletion vectors | Physically resolves soft-deletes; reduces Direct Lake cold-state overhead |

### Lakehouse Maintenance Activity (Preview — March 2026, untested)

The March 2026 Fabric release introduced a native **Lakehouse Maintenance activity** in Data Factory Pipelines. This is a dedicated pipeline activity for table maintenance — potentially replacing the Notebook activity + maintenance notebook approach documented here.

Not yet tested. Unknowns include: whether it supports the same file-size gating logic, whether it respects Fast Optimize, and how it handles VACUUM cadence and Direct Lake framing requirements. Worth evaluating when production-ready.

Source: [Fabric March 2026 Feature Summary](https://blog.fabric.microsoft.com/en-us/blog/fabric-march-2026-feature-summary?ft=All)

### Running OPTIMIZE as a Pipeline Step

Rather than scheduling OPTIMIZE as a separate job, the recommended pattern is to run it as the **last step of your pipeline** — after each load completes. This ties it to the natural cadence of your data changes.

Not every table needs OPTIMIZE every run. Dimension tables, master data, and lookup tables may rarely change, making a daily OPTIMIZE run wasteful. The right gate is **average file size vs target** — only run OPTIMIZE if files are meaningfully below the target for your consumption layer.

```python
def optimize_if_needed(table_path, target_mb=400, tolerance=0.8):
    details = spark.sql(f"DESCRIBE DETAIL '{table_path}'").collect()[0]
    num_files = details['numFiles']

    if num_files == 0:
        print(f"{table_path}: skipped — no files")
        return

    avg_file_size_mb = (details['sizeInBytes'] / num_files) / (1024**2)
    threshold_mb = target_mb * tolerance  # only act if avg is below 80% of target

    if avg_file_size_mb < threshold_mb:
        spark.sql(f"OPTIMIZE '{table_path}'")
        print(f"{table_path}: OPTIMIZE ran — avg file size was {avg_file_size_mb:.0f}MB (target {target_mb}MB)")
    else:
        print(f"{table_path}: skipped — avg file size {avg_file_size_mb:.0f}MB is within tolerance of {target_mb}MB target")
```

> This is a simplified example. The full implementation in `doctor_treatment_table_maintenance` includes before/after file count metrics, a result dict for summary reporting, a single-file skip, an oversized-table skip, and — critically — a liquid clustering bypass that always runs OPTIMIZE on clustered tables regardless of average file size.

To run OPTIMIZE and VACUUM across all tables in a Lakehouse, use `doctor_treatment_maintenance_orchestrator`. It enumerates tables via `mssparkutils.fs.ls()` with `_delta_log` detection — no `SHOW TABLES` required, and handles both schema-enabled and non-schema Lakehouses automatically.

**Notes:**
- For Gold tables serving Direct Lake, ensure this notebook completes before your Power BI dataset refresh so deletion vectors are cleared and liquid clustering is applied before Direct Lake frames the latest commit
- VACUUM on Gold tables: confirm the semantic model has been re-framed to the current Delta version before the weekly run to avoid Direct Lake query errors
- Fast Optimize handles bin-level evaluation within each OPTIMIZE run — the average file size check gates whether to start the command at all; they are complementary

> **Path syntax in Fabric:** SQL statements reference Delta tables via ABFSS path. Use `'{table_path}'` for OPTIMIZE, VACUUM, and DESCRIBE DETAIL. Use `delta.\`{table_path}\`` for ALTER TABLE and DESCRIBE HISTORY. `{table_path}` is the full ABFSS path: `abfss://{workspace_guid}@onelake.dfs.fabric.microsoft.com/{lakehouse_guid}/Tables/{table_name}`.

### DRY RUN

For ad hoc checks or before large backfills, use `DRY RUN` to see what would be compacted without making changes:

```sql
OPTIMIZE '{table_path}' DRY RUN;
```

### Basic OPTIMIZE

```sql
OPTIMIZE '{table_path}';
```

### OPTIMIZE with V-Order (Gold / Direct Lake tables)

```sql
OPTIMIZE '{table_path}' VORDER;
```

### OPTIMIZE with Z-Order

Use when queries frequently filter on specific columns and the table does not use Liquid Clustering:

```sql
OPTIMIZE '{table_path}' ZORDER BY (customer_id, event_date);
```

Note: Z-Order and Liquid Clustering are mutually exclusive. Prefer Liquid Clustering for new tables.

As of March 2026, the Native Execution Engine has native support for both Z-Order and Liquid Clustering. Enabling the Native Execution Engine compounds the I/O benefits of clustering with vectorised execution — no query changes required. See [liquid-clustering.md](./liquid-clustering.md) for benchmark details.

---

## Assessing Table Health

### Target file size by consumption engine

| Engine | Target file size | If files are smaller |
|---|---|---|
| SQL Analytics Endpoint | ~400 MB | Run `OPTIMIZE` |
| Power BI Direct Lake | 400 MB to 1 GB | Run `OPTIMIZE VORDER` |
| Spark | 128 MB to 1 GB (ATFS-managed) | Enable auto-compaction |

### Table history

```sql
DESCRIBE HISTORY delta.`{table_path}`;
```

Auto-compaction runs appear as `OPTIMIZE` with `auto=true` in `operationParameters`.

---

## VACUUM

VACUUM removes Parquet files no longer referenced by the Delta transaction log — files left behind by updates, deletes, or previous OPTIMIZE runs.

### Retention Minimum: 7 Days

**Never set retention below 7 days (168 hours).** Delta's default is 7 days and exists for good reason:

- Any long-running Spark query or streaming reader that opened a snapshot before the VACUUM run could reference files that are now deleted — causing `FileNotFoundException` and potential data corruption
- The 7-day window covers typical overnight batch windows, weekend gaps, and most operational response times
- **Direct Lake:** a framed semantic model references a specific Delta commit version — VACUUM must not remove files from that version until the model has been re-framed to a newer commit

The `retentionDurationCheck` is enabled by default — no configuration change is required. Simply pass `RETAIN 168 HOURS` explicitly:

```sql
VACUUM '{table_path}' RETAIN 168 HOURS;
```

### Recommended VACUUM Cadence

| Layer | Suggested Cadence | Notes |
|---|---|---|
| Bronze | Weekly | High write volume creates orphaned files quickly |
| Silver | Weekly | Balance between reclaiming space and retention window |
| Gold | Weekly | Direct Lake framing means retention window must be respected carefully |

### Before Running VACUUM

1. Confirm no streaming readers or long-running queries are active against the table
2. For Direct Lake tables: confirm the semantic model has been re-framed to the current Delta commit version before running VACUUM. Note: the March 2026 release introduced a dedicated **SQL endpoint refresh activity (Preview)** in Pipelines that may make this step more reliable — untested, worth evaluating
3. Run `DESCRIBE HISTORY` to understand recent activity:

```sql
DESCRIBE HISTORY delta.`{table_path}`;
```

### DRY RUN

```sql
VACUUM '{table_path}' RETAIN 168 HOURS DRY RUN;
```

Returns the list of files that would be deleted without removing anything.

---

## References

- [Cross-Workload Table Maintenance and Optimization — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/fundamentals/table-maintenance-optimization)
- [Understand Direct Lake Query Performance — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-understand-storage)
- [Table Compaction — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/table-compaction)
- [Lakehouse and Delta Tables — Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables)
- [Microsoft Fabric Table Maintenance Optimization — Christopher Finlan (Feb 2026)](https://christopherfinlan.com/2026/02/15/microsoft-fabric-table-maintenance-optimization-a-cross-workload-survival-guide/)
