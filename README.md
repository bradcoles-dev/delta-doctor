# delta-optimizer

**Production-ready Delta table maintenance for Microsoft Fabric.**

Fabric capacity SKUs double in cost at every tier. Poor Delta table maintenance silently inflates that cost over time — tables accumulate small files, deletion vectors build up, liquid clustering goes stale, and queries scan far more data than they need to. The platform is working harder than it should, not because you have more data or more users, but because the tables have never been properly maintained.

This library gives Fabric practitioners the opinionated, medallion-aware automation to fix that.

---

## What it is

**delta-optimizer** is a Fabric Notebook Library — a set of production-ready PySpark notebooks designed to be imported directly into a Microsoft Fabric workspace. Each notebook has a single, well-defined responsibility and is designed to be called from a Fabric Data Factory pipeline or run interactively.

The library is:

- **Opinionated** — sensible defaults per medallion layer (Bronze / Silver / Gold), not a blank configuration surface
- **Safe** — OPTIMIZE is gated on actual table health; VACUUM respects the 7-day minimum retention floor
- **Transparent** — every decision is logged; DRY RUN support before any write operation
- **Incremental** — built around Microsoft's recommended Fast Optimize, Auto-Compaction, and Adaptive Target File Size so maintenance costs almost nothing when tables are already healthy

---

## The Library

| Notebook | Purpose | Typical caller |
|---|---|---|
| `dopt_utility_session_config` | Sets up a Spark session with the correct baseline configurations for a given medallion layer | Called at the top of every pipeline notebook |
| `dopt_utility_table_health` | Scans all tables in a Lakehouse and produces a health report — file counts, average file sizes, fragmentation status, deletion vector state, clustering state | Run interactively or as a pipeline step |
| `dopt_utility_table_maintenance` | Runs OPTIMIZE (if needed) and VACUUM (weekly or forced) on a single table, parameterised by the calling pipeline | Called as the final step of each pipeline load |
| `dopt_utility_maintenance_orchestrator` | Iterates across all tables in a Lakehouse and calls `dopt_utility_table_maintenance` for each, with layer-aware defaults | Scheduled pipeline; useful before adopting per-table pipeline calls |
| `dopt_utility_set_table_properties` | Sets Delta table properties (deletion vectors, auto-compaction, optimize write, V-Order) on a single table based on its medallion layer. Properties persist across sessions — correct for multi-writer tables | Run once per table at setup time, or called from an onboarding pipeline |
| `dopt_utility_set_properties_orchestrator` | Iterates all tables in a Lakehouse and calls `dopt_utility_set_table_properties` for each. Run once per Lakehouse at onboarding time, passing the matching `layer` for that Lakehouse | One-off onboarding pipeline; run once per medallion Lakehouse |

> **Status:** The library is under active development. See [Roadmap](#roadmap) below.

---

## Design Principles

**Maintenance runs should cost nothing when tables are healthy.**
Every OPTIMIZE call is gated on a metadata check (`DESCRIBE DETAIL`) — no data scan, runs in seconds. If the average file size is within tolerance of the layer target, the table is skipped. Microsoft's Fast Optimize handles bin-level evaluation within each run.

**Layer targets are explicit, not implicit.**
Bronze targets 128 MB. Silver targets 256 MB. Gold targets 400 MB. These are passed as parameters, not buried in defaults.

**The 7-day VACUUM floor is non-negotiable.**
VACUUM will never run with a retention window below 168 hours. The library enforces this in code — it is not a documentation note you might miss.

**Direct Lake coordination matters.**
For Gold tables serving Power BI Direct Lake, VACUUM must run *after* the semantic model has re-framed to the latest Delta commit. The orchestrator accounts for this; the maintenance notebook documents it.

**Session configs belong in one place.**
The session config notebook sets the full baseline — Auto-Compaction, ATFS, Fast Optimize, File Level Compaction Target, and explicit Optimize Write and V-Order values. Per-notebook overrides apply on top. No notebook should rely on undocumented workspace defaults.

---

## Getting Started

> **Prerequisites:** Microsoft Fabric workspace with a Lakehouse and Spark runtime (Runtime 1.3 or Runtime 2.0).

1. Download or clone this repository
2. Import the notebooks into your Fabric workspace via **Import notebook** in the Data Engineering experience
3. Start with `dopt_utility_table_health` — run it against your Lakehouse to see the current state of your tables before changing anything
4. Wire `dopt_utility_table_maintenance` as the final activity in your existing pipeline notebooks, passing `lakehouse_guid`, `table_name`, `target_mb`, and `force_vacuum` as parameters
5. Add a call to `dopt_utility_session_config` at the top of each pipeline notebook, passing the layer as a parameter

Detailed setup guides are in [`/docs`](./docs/).

---

## Roadmap

### v0.1 — Fabric Notebook Library *(current)*
Five notebooks covering session config, table health scanning, single-table maintenance, Lakehouse-wide orchestration, and table property management. Deployable directly into any Fabric workspace.

### v0.2 — Observability
Maintenance history logging to a Delta table. Per-table trend tracking — file count, average file size, OPTIMIZE/VACUUM run history. Enables dashboarding in Power BI.

A Fabric SQL database control table mapping `table_name → layer` is also planned for v0.2. This would allow `dopt_utility_set_properties_orchestrator` to apply per-table layer overrides (e.g. a Silver table configured with Gold properties for Direct Lake), removing the current assumption that all tables in a Lakehouse share the same layer.

### v0.3 — Intelligence
Auto-detection of table type (append-only vs MERGE-heavy) to recommend and apply appropriate settings. Cluster key recommendations based on column cardinality and query patterns (where accessible).

### v1.0 — Python Package
`pip`-installable. Works inside Fabric notebooks and local development. Stable public API. The notebook library becomes a thin wrapper over the package.

### Beyond v1.0
Productised offering under evaluation — managed deployment, workspace-level scheduling, cost analytics.

---

## Background

This library emerged from research published in:

> **[Don't Double Your Microsoft Fabric Spend. Just Set These Configs.](https://www.linkedin.com/in/brad-coles/)** — Brad Coles

The article covers the full theory — what Fabric does and does not automate, the right settings by medallion layer, liquid clustering vs partitioning, deletion vector management, and VACUUM retention decisions. This library is the engineering implementation of those recommendations.

---

## Contributing

Contributions, issues, and discussion are welcome. If you are a Fabric practitioner using this library in production, feedback on real-world behaviour is especially valuable — open an issue or start a discussion.

---

## License

Apache 2.0
