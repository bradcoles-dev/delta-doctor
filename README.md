# delta-doctor

[![Version](https://img.shields.io/badge/version-v0.1-blue)](https://github.com/bradcoles-dev/delta-doctor/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

**Production-ready Delta table maintenance for Microsoft Fabric.**

Fabric capacity SKUs double in cost at every tier. Poor Delta table maintenance silently inflates that cost over time — tables accumulate small files, [deletion vectors](docs/deletion-vectors.md) build up, [liquid clustering](docs/liquid-clustering.md) goes stale, and queries scan far more data than they need to. The platform is working harder than it should, not because you have more data or more users, but because the tables have never been properly maintained.

This affects any Fabric Lakehouse, regardless of how data gets in. Whether you write with Spark notebooks, dbt-fabric, Dataflow Gen2, Copy activity, or the Fabric Python kernel (DuckDB or Polars), the resulting Delta tables have identical maintenance needs. Python kernel tables are in worse shape by default — no auto-compaction fires and VACUUM never runs automatically. This library gives you safe, automated Delta maintenance with layer-specific defaults for Bronze, Silver, and Gold — no per-table manual configuration required.

---

## What it is

**delta-doctor** is a Fabric Notebook Library — a collection of Spark notebooks you import directly into your Fabric workspace via the **Import notebook** button in the Data Engineering experience. There is no installation command or package manager. Each notebook has a single, well-defined responsibility and is designed to be called from a Fabric Data Factory pipeline or run interactively.

Delta table maintenance sits below the transformation layer — it applies to the tables themselves, not to whatever wrote them. If you use dbt-fabric, Dataflow Gen2, or Copy activity for your transformations, this library is still fully applicable. The one exception is Fabric Warehouse, which manages its own storage automatically.

The design decisions behind the library — layer targets, ATFS behaviour, V-Order trade-offs, VACUUM retention — are covered in [Delta Table Maintenance in Microsoft Fabric: A 2026 Practitioner's Guide](https://bradcoles.dev/blog/fabric-delta-table-maintenance.html).

The library is:

- **Layer-driven** — defaults are set per medallion layer (Bronze / Silver / Gold) and applied without per-table configuration; a custom mode supports non-medallion architectures
- **Safe** — OPTIMIZE is gated on actual table health; VACUUM respects the 7-day minimum retention floor
- **Transparent** — every decision is logged; no silent skips or silent failures
- **Incremental** — built around Microsoft's recommended Fast Optimize (bin-level compaction that skips already-healthy files), Auto-Compaction, and Adaptive Target File Size so maintenance costs almost nothing when tables are already healthy

---

## The Library

delta-doctor is organised around three pillars: **Diagnosis**, **Treatment**, and **Preventative Care**.

### Diagnosis
*Understand what your tables look like before you change anything.*

| Notebook | Purpose | Typical caller |
|---|---|---|
| `doctor_diagnosis_table_health` | Scans all tables in a Lakehouse and produces a health report — file counts, average file sizes, fragmentation status, deletion vector state, clustering state. Classifies each table as Healthy, Review, Needs OPTIMIZE, Oversized, or a skip status | Run interactively before onboarding, or any time you want a current picture |

### Treatment
*Fix what is broken and restore tables to a healthy baseline.*

| Notebook | Purpose | Typical caller |
|---|---|---|
| `doctor_treatment_table_maintenance` | Runs OPTIMIZE (if needed) and VACUUM (weekly or forced) on a single table. Logs before/after file counts and average file size when OPTIMIZE runs | Called as the final step of each pipeline load |
| `doctor_treatment_maintenance_orchestrator` | Iterates all tables in a Lakehouse, running OPTIMIZE and VACUUM on each. Logs before/after metrics per table and prints a run summary including total files compacted | Scheduled pipeline; useful before adopting per-table pipeline calls |
| `doctor_treatment_rebaseline_orchestrator` | Runs `REORG TABLE APPLY (PURGE)` followed by OPTIMIZE on every table in a Lakehouse, rewriting all files to the correct layer target and purging accumulated deletion vectors | One-off rebaseline on a neglected Lakehouse; run once, then hand off to the maintenance orchestrator |

### Preventative Care
*Configure tables and sessions correctly so problems do not reoccur.*

| Notebook | Purpose | Typical caller |
|---|---|---|
| `doctor_prevention_session_config` | Sets up a Spark session with the correct baseline configurations for a given medallion layer | Called at the top of every pipeline notebook |
| `doctor_prevention_set_table_properties` | Sets Delta table properties (deletion vectors, auto-compaction, optimize write, V-Order, target file size) on a single table by layer. Supports a custom mode for non-medallion tables. Optionally enables liquid clustering | Run once per table at setup time, or called from an onboarding pipeline |
| `doctor_prevention_set_properties_orchestrator` | Iterates all tables in a Lakehouse and calls `doctor_prevention_set_table_properties` for each. Run once per Lakehouse at onboarding time | One-off onboarding pipeline; run once per medallion Lakehouse |

> **Schema support:** All notebooks use ABFSS paths and handle both schema-enabled Lakehouses (`Tables/{schema}/{table}`) and non-schema Lakehouses (`Tables/{table}`) automatically.

> **Status:** v0.1 is complete and ready to deploy once the [deployment validation guide](validation/deployment-validation.md) has been passed against your workspace. See [Roadmap](#roadmap) for what's coming.

---

## Design Principles

**Maintenance runs should cost nothing when tables are healthy.**
Every OPTIMIZE call is gated on a metadata check (`DESCRIBE DETAIL`) — no data scan, runs in seconds. If the average file size is within tolerance of the layer target, the table is skipped. Microsoft's Fast Optimize handles bin-level evaluation within each run.

**Layer targets are explicit, not implicit.**
Bronze targets 128 MB. Silver targets 256 MB. Gold targets 400 MB. These are passed as parameters, not buried in defaults.

The `target_mb` parameter in the maintenance notebooks is the *threshold for whether to call OPTIMIZE* — not the output file size. Adaptive Target File Size (ATFS) controls the actual output size when OPTIMIZE runs. `doctor_prevention_set_table_properties` sets `delta.targetFileSize` as a table property to give ATFS a per-table ceiling to adapt from, while ATFS adapts downward for small tables to avoid pathological results.

The targets that matter most for correctness are **Silver and Gold**. Silver at 256 MB balances Spark processing efficiency for transformation workloads. Gold at 400 MB is critical — the SQL Analytics Endpoint and Power BI Direct Lake have genuine performance dependencies on file size. Bronze at 128 MB is a pragmatic default, not a hard requirement: Bronze tables are read by Spark notebooks, not by Direct Lake or the SQL Endpoint, and the difference between 80 MB and 128 MB files at Bronze has no meaningful query performance impact. The priority at Bronze is preventing small file accumulation, not hitting an exact size.

**The 7-day VACUUM floor is non-negotiable.**
VACUUM will never run with a retention window below 168 hours. The library enforces this in code — it is not a documentation note you might miss.

**Direct Lake coordination matters.**
For Gold tables serving Power BI Direct Lake, VACUUM must run *after* the semantic model has re-framed to the latest Delta commit. The orchestrator accounts for this; the maintenance notebook documents it.

**Session configs belong in one place.**
The session config notebook sets the full baseline — Auto-Compaction, ATFS, Fast Optimize, File Level Compaction Target, and explicit Optimize Write and V-Order values. Per-notebook overrides apply on top. No notebook should rely on undocumented workspace defaults.

**Table properties beat session configs for shared tables.**
Session configs apply only to the current notebook session. For tables written by multiple pipelines or notebooks, Delta table properties are set once and apply regardless of which session writes. The property notebooks enforce this distinction.

---

## Getting Started

> **Prerequisites:** Microsoft Fabric workspace with a Lakehouse and Spark runtime (Runtime 1.3 or later; Runtime 2.0 strongly recommended if using liquid clustering — see `docs/liquid-clustering.md` for material behavioural differences on Runtime 1.3). All notebooks must reside in the same Fabric workspace as the target Lakehouse — the workspace GUID is derived automatically at runtime via `mssparkutils.env.getWorkspaceId()`. The identity running the notebooks must have at least **Contributor** permissions on the Fabric workspace and write access to the target Lakehouse.

> **Finding your Lakehouse GUID:** Open your Lakehouse in the Fabric UI and look at the browser URL. It follows the pattern `https://app.powerbi.com/groups/{workspace-guid}/lakehouses/{lakehouse-guid}`. For example: `https://app.powerbi.com/groups/6f9762f2-154f-4786-92c2-93b6b51e0401/lakehouses/4eb10241-c8b8-4778-b905-a36005890601` — the workspace GUID is `6f9762f2-...` and the Lakehouse GUID is `4eb10241-...`. The Lakehouse GUID is the `lakehouse_guid` parameter used throughout the library.

1. Download or clone this repository
2. Import the notebooks into your Fabric workspace via **Import notebook** in the Data Engineering experience
3. Start with `doctor_diagnosis_table_health` — pass `lakehouse_guid` as a parameter and run it to see the current state of your tables before changing anything
4. Run `doctor_prevention_set_properties_orchestrator` once per Lakehouse to set the correct Delta table properties for every table. Pass the `lakehouse_guid` and the `layer` for that Lakehouse
5. Run `doctor_treatment_rebaseline_orchestrator` once to right-size all files to the layer target and purge accumulated deletion vectors. On a previously unmaintained Lakehouse this is an expensive one-off — expect at least minutes per table. Monitor progress in the Spark UI. Once complete, use `doctor_treatment_maintenance_orchestrator` for scheduled ongoing maintenance

> Step 3 is a diagnostic — re-run any time you want a current picture. Steps 4–5 are one-off setup — run once when onboarding a Lakehouse. Steps 6–7 are the ongoing pattern — wired into every pipeline going forward.

6. Add a call to `doctor_prevention_session_config` at the top of each pipeline notebook, passing the layer as a parameter
7. Wire `doctor_treatment_table_maintenance` as the final activity in each pipeline going forward. Required parameters: `lakehouse_guid`, `table_name`, `layer`. Optional: `schema_name` (schema-enabled Lakehouses only), `force_vacuum` (default `False`; set `True` for ad-hoc runs after large backfills). When `layer = "custom"`, `custom_target_mb` is also required — must be a positive integer specifying the target file size in MB

> **Pipeline return values:** These notebooks print all decisions to Spark stdout (visible in pipeline run logs) but do not return structured values via `mssparkutils.notebook.exit()`. Pipeline branching on individual maintenance outcomes is not currently supported.

Detailed setup guides are in [`/docs`](./docs/).

---

## Roadmap

### v0.1 — Fabric Notebook Library *(current)*
Seven notebooks covering session config, table health scanning, single-table maintenance, Lakehouse-wide maintenance orchestration, table property management, Lakehouse-wide property orchestration, and one-off Lakehouse rebaselining. Deployable directly into any Fabric workspace.

### v0.2 — Observability
**Health history logging.** `doctor_diagnosis_table_health` gains an optional `history_lakehouse_guid` parameter. When provided, it appends the full Lakehouse snapshot to a `doctor_table_health_history` Delta table at the end of each run — one write, one clean timestamped snapshot per execution. When empty, the notebook behaves exactly as it does today (interactive display only), preserving the existing use case.

The history table schema is the existing health report columns plus `run_timestamp`. Scheduled daily, this produces a per-table trend record over time: file count, average file size, fragmentation status, deletion vector state, clustering state.

**Power BI dashboard.** A Direct Lake semantic model built on `doctor_table_health_history` enables a Delta table health monitoring dashboard — trend lines per table, status breakdowns, tables that are degrading between runs. No additional infrastructure beyond the existing Lakehouse.

**Control table.** A Delta table mapping `table_name → layer` to allow `doctor_prevention_set_properties_orchestrator` to apply per-table layer overrides (e.g. a Silver table configured with Gold properties for Direct Lake), removing the current assumption that all tables in a Lakehouse share the same layer.

**File size distribution.** The v0.1 maintenance notebooks use average file size as a proxy for compaction decisions. A bimodal distribution of small and large files (e.g. three 200 MB files and one 1 GB file on a 400 MB Gold target) may not be detected correctly by the average alone. Health history data enables per-file size distribution analysis — improving REORG gating accuracy — planned for v0.2.

**Gates:**
- *Entry:* Each feature above is specified in enough detail to build — history table schema finalised, control table schema and lookup behaviour defined, Power BI dashboard requirements documented
- *Exit:* Validation guide written covering all v0.2 features; guide passes against a real Fabric workspace

### v0.3 — Intelligence
Auto-detection of table type (append-only vs MERGE-heavy) to recommend and apply appropriate settings. Cluster key recommendations based on column cardinality and query patterns (where accessible).

**Gates:**
- *Entry:* Detection approach defined — which Delta log or table statistics signals distinguish append-only from MERGE-heavy; which metadata is available in Fabric to support cluster key recommendations
- *Exit:* Validation guide written covering all v0.3 features; guide passes against a real Fabric workspace

### v1.0 — Python Package
`pip`-installable. Works inside Fabric notebooks and local development. Stable public API. The notebook library becomes a thin wrapper over the package.

The package exposes a clean programmatic API — `diagnose(lakehouse_guid)`, `optimize(table_name)`, `set_properties(table_name, layer)` — callable from any Python environment, not just Fabric notebooks. This is the foundation for everything above v1.0.

### v2.0 — GUI
A user-facing interface that wraps the Python package API. The practitioner enters a Lakehouse GUID, selects a layer, and clicks **Diagnose**. The tool returns the health report — file counts, fragmentation status, deletion vector state, clustering state — and surfaces specific recommendations: which tables need OPTIMIZE, which need properties set, which are healthy. The practitioner reviews and clicks **Apply**. No notebook authoring required.

The most natural Fabric-native surface is a Power App or Fabric workload extension. The diagnosis → recommendation → apply loop maps directly onto the existing library:
- Diagnose → `doctor_diagnosis_table_health`
- Recommend → status column logic (`Needs OPTIMIZE`, `Review`, `Healthy`)
- Apply → `doctor_treatment_maintenance_orchestrator` + `doctor_prevention_set_properties_orchestrator`

### Beyond v2.0 — Fabric-Native App (Rayfin)
Rather than a traditionally-hosted external service, the productised offering is a Fabric-native app built on [Rayfin](https://www.microsoft.com/en-us/microsoft-fabric/features/rayfin) — Microsoft's open-source SDK for code-first backends on Fabric (announced Build 2026).

A Rayfin app removes the trust and infrastructure problems of an external hosted service:
- The app deploys as a first-class artifact inside the user's own Fabric workspace — no OAuth, no external data transfer, governance inherited automatically
- Health scan results land directly in OneLake, making the history table immediately available to Power BI Direct Lake with no additional pipeline work
- The app itself can be open-sourced on top of the delta-doctor Python package

The architecture is a clean layer split: the Rayfin app handles the UI, API, and orchestration layer in Python; the delta-doctor Python package (v1.0) handles the Spark operations underneath. Delta DDL commands (`OPTIMIZE`, `VACUUM`, `ALTER TABLE`) require Spark and always will — the Python package wraps these and is called by the Rayfin layer via the Fabric REST API or directly within a Spark context.

---

## Background

This library emerged from research published in:

> **[Delta Table Maintenance in Microsoft Fabric: A 2026 Practitioner's Guide](https://bradcoles.dev/blog/fabric-delta-table-maintenance.html)** — Brad Coles

The article covers the full theory — what Fabric does and does not automate, the right settings by medallion layer, liquid clustering vs partitioning, deletion vector management, and VACUUM retention decisions. This library is the engineering implementation of those recommendations.

---

## Contributing

Contributions, issues, and discussion are welcome. Feedback on real-world behaviour — edge cases, unexpected Spark runtime differences, table configurations that don't fit the layer model — is particularly valuable. Open an issue or start a discussion.

---

## License

Apache 2.0
