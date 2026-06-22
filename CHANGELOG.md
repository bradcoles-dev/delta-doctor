# Changelog

## v0.1 — 2026-06-22

Initial release. Seven production-ready notebooks for Delta table maintenance on Microsoft Fabric.

### Notebooks

| Notebook | Responsibility |
|---|---|
| `doctor_prevention_session_config` | Spark session baseline by medallion layer |
| `doctor_diagnosis_table_health` | Health report across all tables in a Lakehouse |
| `doctor_treatment_table_maintenance` | OPTIMIZE + VACUUM on a single table |
| `doctor_treatment_maintenance_orchestrator` | OPTIMIZE + VACUUM across all tables in a Lakehouse |
| `doctor_prevention_set_table_properties` | Delta table properties + liquid clustering on a single table |
| `doctor_prevention_set_properties_orchestrator` | Table properties across all tables in a Lakehouse |
| `doctor_treatment_rebaseline_orchestrator` | One-off REORG TABLE APPLY (PURGE) + OPTIMIZE to reset file sizes to the layer target |

### Key behaviours

- OPTIMIZE gating: skips tables whose average file size is already within 80% of the layer target
- VACUUM floor: 168 hours enforced in code on every path — not just documented
- Direct Lake gate: Gold-layer VACUUM raises `ValueError` unless `direct_lake_confirmed = True`
- Schema-enabled Lakehouses: all orchestrators handle `Tables/{schema}/{table}` structure via `_delta_log` detection
- Oversized table detection: warns when average file size exceeds 2× target and recommends rebaseline
- Health statuses: `Healthy`, `Within tolerance` (80–100% of target), `Review` (50–80%), `Needs OPTIMIZE`, `Oversized`, `Skip - single file`, `Skip - empty table`, `No target set`
