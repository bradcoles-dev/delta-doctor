# Cold-Read Audit Prompt

A structured prompt for running a cross-file consistency audit of the delta-optimizer project using an AI agent. Run this at release boundaries or after large batches of changes — not continuously.

## How to use

Paste the prompt below into a fresh Claude Code session (or use the Agent tool) and point it at the repo root. The agent reads every file from scratch and reports real issues only.

Update the **Known non-issues** section whenever you make a deliberate decision that the agent would otherwise flag repeatedly.

---

## Prompt

```
You are doing a cold-read audit of the delta-optimizer project — a Fabric Notebook Library for Delta table maintenance on Microsoft Fabric. Your job is to read every file from scratch and find real issues: bugs, contradictions, broken invariants, documentation that disagrees with code. Do NOT flag cosmetic preferences or style opinions.

## What to audit

Read ALL of the following files in full:

**Notebooks (notebook-content.py in each .Notebook folder):**
- `Notebooks/dopt_utility_session_config.Notebook/notebook-content.py`
- `Notebooks/dopt_utility_table_health.Notebook/notebook-content.py`
- `Notebooks/dopt_utility_table_maintenance.Notebook/notebook-content.py`
- `Notebooks/dopt_utility_maintenance_orchestrator.Notebook/notebook-content.py`
- `Notebooks/dopt_utility_set_table_properties.Notebook/notebook-content.py`
- `Notebooks/dopt_utility_set_properties_orchestrator.Notebook/notebook-content.py`
- `Notebooks/dopt_utility_rebaseline_orchestrator.Notebook/notebook-content.py`

**Docs:**
- `docs/optimize-vacuum.md`
- `docs/compaction-and-file-management.md`
- `docs/deletion-vectors.md`
- `docs/liquid-clustering.md`
- `docs/v-order.md`
- `docs/medallion-recommendations.md`

**Other:**
- `README.md`
- `CONTRIBUTING.md`
- `validation/deployment-validation.md`
- `CLAUDE.md`

## Known non-issues — do NOT flag these

- `list_delta_tables()` defined identically in four notebooks — intentional, documented in CLAUDE.md (table_health, maintenance_orchestrator, set_properties_orchestrator, rebaseline_orchestrator)
- The inner `except Exception: pass` in `list_delta_tables()` schema subfolder recursion — intentional silence for non-Delta directories
- The `mssparkutils.notebook.run()` timeout of 120 seconds in `set_properties_orchestrator` — documented in the notebook header
- The `optimize-vacuum.md` code block omitting the single-file skip — documented inline as a simplified example
- `optimizeWrite` table property always set to `true` for Silver/Gold — intentional; append-only pipelines disable at session level via `dopt_utility_session_config`
- The partition check in `set_table_properties` only fires when `cluster_by` is non-empty — correct; the section markdown accurately describes it as a guard for the clustering DDL path

## What to check

**Cross-notebook consistency:**
- Do all four `list_delta_tables()` copies have identical function bodies? (table_health, maintenance_orchestrator, set_properties_orchestrator, rebaseline_orchestrator)
- Do both `optimize_if_needed()` copies handle `num_files == 0` and `num_files == 1` as distinct early exits with distinct messages? (table_maintenance, maintenance_orchestrator)
- Do both `optimize_if_needed()` skip-tolerance messages include the target MB value?
- Do both `vacuum_table()` copies enforce `retain_hours = max(retain_hours, 168)`?
- Are LAYER_TARGETS consistent across all notebooks that define them?

**VACUUM floor:**
- Is `retain_hours = max(retain_hours, 168)` present in every `vacuum_table()`?

**ABFSS paths:**
- All table references use ABFSS paths, not catalog-style naming?
- `mssparkutils.env.getWorkspaceId()` used everywhere the workspace GUID is needed?

**Parameter correctness:**
- Does `dopt_utility_table_maintenance` raise `ValueError` when `layer = "custom"` and `custom_target_mb` is 0?
- Does `dopt_utility_session_config` validate `custom_optimize_write` and `custom_v_order` as strings when `layer = "custom"`?
- Does `dopt_utility_set_table_properties` check partition columns before any DDL when `cluster_by` is set?
- Does `dopt_utility_table_health` warn when `custom_target_mb > 0` with a non-custom layer?

**Documentation accuracy:**
- Does the validation guide expected output for tests 1.1–1.4 match all print statements the notebook actually emits?
- Does the validation guide test 2.1 status list include all statuses the code can produce?
- Does `medallion-recommendations.md` accurately reflect what the notebooks actually do?
- Do doc examples use ABFSS path patterns, not catalog-style names?
- Does the README accurately describe required vs optional parameters for each notebook?

**Error handling:**
- Any bare `except: pass` blocks? (should be `except Exception:` at minimum)
- Does the outer loop of `list_delta_tables()` log a warning for top-level enumeration failures?

**Status classification (table_health):**
- Are `num_files == 0` and `num_files == 1` handled as distinct statuses (`Skip - empty table` vs `Skip - single file`)?
- Does the status values documentation in the parameters markdown list all statuses in code order?

**Structure:**
- Every notebook has a `# ## Validation` markdown cell before its validation code cell?
- Parameters in PARAMETERS CELL match parameters markdown table for all seven notebooks (no extras, no missing)?

**Other:**
- Any contradictions between a notebook header and its actual code?
- Any expected output in the validation guide that contradicts actual code behaviour?

## Output format

List every real issue found. For each:
- **File:** which file
- **Severity:** High / Medium / Low
- **Issue:** one sentence
- **Evidence:** exact line or code that is wrong and what it should be

If a category is clean, say so in one line. If there are NO issues at all, say so clearly.

Working directory: `C:\Users\bradc\OneDrive\Desktop\Git Repos\delta-optimizer`
```
