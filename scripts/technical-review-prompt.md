# Technical Review Prompt

A structured prompt for stress-testing the technical correctness of delta-doctor from the perspective of a senior Delta Lake / Microsoft Fabric engineer. Run this before any public release or after significant changes to maintenance logic, session config, or table property decisions.

## How to use

Paste the prompt below into a fresh Claude Code session (or use the Agent tool) and point it at the repo root. The agent evaluates whether the technical decisions, SQL operations, and Spark configurations are correct, well-reasoned, and complete.

The goal is to surface unsound design decisions, incorrect technical claims, and unhandled edge cases. Do not add findings to a suppression list — if a design decision is flagged, either the decision or its documentation needs to improve.

---

## Prompt

```
You are a principal engineer with deep expertise in Delta Lake internals, Apache Spark, and Microsoft Fabric. You have built and operated large-scale Delta table maintenance systems in production. You are doing a technical review of delta-doctor — a Fabric Notebook Library for Delta table maintenance.

Your job is to evaluate whether the technical decisions, Spark configurations, SQL operations, and maintenance logic are correct and well-reasoned. Flag anything that is wrong, incomplete, or likely to cause problems in production. Do NOT flag cosmetic or style issues.

## What to read

**All notebooks:**
- `Notebooks/doctor_prevention_session_config.Notebook/notebook-content.py`
- `Notebooks/doctor_diagnosis_table_health.Notebook/notebook-content.py`
- `Notebooks/doctor_treatment_table_maintenance.Notebook/notebook-content.py`
- `Notebooks/doctor_treatment_maintenance_orchestrator.Notebook/notebook-content.py`
- `Notebooks/doctor_prevention_set_table_properties.Notebook/notebook-content.py`
- `Notebooks/doctor_prevention_set_properties_orchestrator.Notebook/notebook-content.py`
- `Notebooks/doctor_treatment_rebaseline_orchestrator.Notebook/notebook-content.py`

**Docs:**
- `docs/optimize-vacuum.md`
- `docs/compaction-and-file-management.md`
- `docs/deletion-vectors.md`
- `docs/liquid-clustering.md`
- `docs/v-order.md`
- `docs/medallion-recommendations.md`

**Other:**
- `README.md`
- `CLAUDE.md`

## What to assess

**Session configuration**
- Are the Spark session configs in `doctor_prevention_session_config` correct for Fabric Runtime 1.3+?
- Is the baseline + override pattern sound? Are there any configs that should not be set at session level?
- Is `spark.sql.caseSensitive = true` appropriate as a global baseline, or could it cause issues?
- Is the interaction between `optimizeWrite` (session config) and `delta.autoOptimize.optimizeWrite` (table property) correctly understood and documented?

**OPTIMIZE logic**
- Is the `avg_mb >= threshold_mb` gate (80% of target) the right heuristic for skipping OPTIMIZE?
- Is average file size a sufficient proxy for compaction decisions? Are the limitations correctly documented?
- Does `optimize_if_needed()` handle all meaningful edge cases (empty table, single file, oversized)?
- Is Fast Optimize (`spark.microsoft.delta.optimize.fast.enabled`) correctly described in docs and headers?

**REORG TABLE APPLY (PURGE)**
- Is REORG TABLE APPLY (PURGE) the correct operation for purging deletion vectors and right-sizing oversized files?
- Is the sequence (REORG then OPTIMIZE) correct? Would OPTIMIZE alone after REORG be sufficient for file sizing?
- Is the oversized threshold (avg > 2× target) appropriate for triggering a full rewrite?
- Are the limitations of using avg file size as the REORG trigger correctly documented?

**VACUUM**
- Is the 168-hour (7-day) retention floor correct and sufficient?
- Is the weekly-on-Sunday cadence safe given the 7-day retention floor?
- Is the Direct Lake re-framing requirement correctly described and placed?

**Delta table properties**
- Are the properties set by `doctor_prevention_set_table_properties` correct for each layer?
- Is `delta.targetFileSize` set in bytes correctly (128 * 1024 * 1024, etc.)?
- Is the interaction between `delta.targetFileSize` and ATFS (`spark.microsoft.delta.targetFileSize.adaptive.enabled`) correctly described?
- Is `delta.enableDeletionVectors = true` safe to apply unconditionally across all layers?
- Does the protocol upgrade warning for deletion vectors accurately describe the compatibility risk?

**Liquid clustering**
- Is `ALTER TABLE CLUSTER BY` the correct DDL for enabling liquid clustering in Fabric?
- Is it correct that clustering is not physically applied until OPTIMIZE runs?
- Is the partition column check (DESCRIBE DETAIL before any DDL) the right guard against enabling clustering on a partitioned table?

**Table enumeration**
- Is `mssparkutils.fs.ls()` + `_delta_log` detection a reliable way to enumerate Delta tables in Fabric?
- Are there table or directory structures that `list_delta_tables()` would incorrectly enumerate or miss?
- Is the schema-enabled Lakehouse recursion logic (`Tables/{schema}/{table}`) correct?

**Layer targets**
- Are the Bronze (128 MB), Silver (256 MB), Gold (400 MB) targets well-justified for Fabric?
- Is the 80% tolerance threshold for OPTIMIZE gating appropriate at each layer?

**Docs correctness**
- Are any technical claims in the docs incorrect or outdated for Fabric Runtime 1.3+?
- Are any Spark config keys incorrect or deprecated?
- Is the path syntax guidance (single-quote vs backtick/delta-prefix) correct for each SQL operation?

## Output format

For each finding:
- **Area:** which file or section
- **Severity:** High (incorrect / will cause production issues) / Medium (suboptimal or incomplete) / Low (minor inaccuracy)
- **Finding:** one sentence describing the issue
- **Evidence:** the specific config, SQL, or claim that is wrong, and what it should be

If a technical decision is sound and well-reasoned, say so in one line. Be direct — incorrect technical decisions in a maintenance library can cause data loss or performance regressions.

Working directory: `C:\Users\bradc\OneDrive\Desktop\Git Repos\delta-doctor`
```
