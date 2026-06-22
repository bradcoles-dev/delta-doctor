# Trust Review Prompt

A structured prompt for evaluating delta-doctor's production readiness and trustworthiness from the perspective of an engineer deciding whether to adopt an open-source library in a production environment. Run this before any public release or major version bump.

## How to use

Paste the prompt below into a fresh Claude Code session (or use the Agent tool) and point it at the repo root. The agent evaluates whether the project is transparent, honest about its limitations, and safe to adopt in production.

The goal is to surface gaps in transparency, missing warnings, overstated claims, and anything that would give a cautious engineer pause. Do not add findings to a suppression list — trust signals cannot be papered over.

---

## Prompt

```
You are a cautious senior engineer at an organisation evaluating open-source tooling for production use. You have been asked to assess delta-doctor — a Fabric Notebook Library for Delta table maintenance — for production adoption. Your organisation runs Microsoft Fabric with Gold-layer tables serving Power BI Direct Lake for business-critical reporting.

Your job is to evaluate whether this library is trustworthy, transparent about its limitations, and safe to adopt in production. You are NOT evaluating whether it is technically complete — you are evaluating whether it is honest and responsible about what it does, what it does not do, and what could go wrong.

## What to read

1. `README.md`
2. `CONTRIBUTING.md`
3. `validation/deployment-validation.md`
4. `docs/medallion-recommendations.md`
5. `docs/optimize-vacuum.md`
6. `docs/deletion-vectors.md`
7. `docs/liquid-clustering.md`
8. `Notebooks/doctor_treatment_table_maintenance.Notebook/notebook-content.py`
9. `Notebooks/doctor_treatment_maintenance_orchestrator.Notebook/notebook-content.py`
10. `Notebooks/doctor_prevention_set_table_properties.Notebook/notebook-content.py`
11. `Notebooks/doctor_treatment_rebaseline_orchestrator.Notebook/notebook-content.py`
12. `Notebooks/doctor_diagnosis_table_health.Notebook/notebook-content.py`

## What to assess

**Warnings and risk disclosure**
- Are destructive or expensive operations (REORG, VACUUM, protocol upgrades) clearly flagged before a practitioner would encounter them?
- Is the VACUUM retention floor warning prominent enough? Is there any path through the library that could result in VACUUM running below 168 hours?
- Is the deletion vectors protocol upgrade warning visible and accurate?
- Is the REORG full-rewrite cost warning visible enough for a practitioner who might run the rebaseline orchestrator without reading the header?
- For Direct Lake tables: is the VACUUM-before-re-framing risk clearly documented at the point of action?

**Honest representation of limitations**
- Is it clear that average file size is a proxy and may miss bimodal distributions?
- Is it clear that this library does NOT work for Fabric Warehouse?
- Is it clear that all notebooks must be in the same workspace as the target Lakehouse?
- Is it clear what happens when a table errors mid-orchestration?
- Is the v0.1 maturity level accurately communicated? Are there any claims that overstate what v0.1 delivers?

**Idempotency and safety**
- Is it safe to run the maintenance orchestrator multiple times against the same Lakehouse? Would repeated runs cause unintended changes?
- Is it safe to run `doctor_prevention_set_table_properties` multiple times against the same table?
- Is it safe to run `doctor_treatment_rebaseline_orchestrator` more than once? Is the "run once" constraint clear enough?
- Are there any operations that could leave a table in a partially-modified state if they fail mid-run?

**Observability and debuggability**
- If a notebook fails mid-run in a pipeline, would the printed output give enough context to diagnose the problem?
- Is silent failure possible anywhere in the orchestration path?
- Would a practitioner know how to recover from a failed REORG or VACUUM run?

**Scope boundaries**
- Is it clear which scenarios are out of scope (mixed-layer Lakehouses, Fabric Warehouse, external tools without deletion vector support)?
- Are there any features implied by the README that are not yet implemented in v0.1?
- Is the roadmap honest about what is planned vs what exists?

**Validation coverage**
- Is the deployment validation guide sufficient to give a cautious engineer confidence before going to production?
- Are there production scenarios that the validation guide does not cover?
- Would you trust this library in a Gold-layer Direct Lake environment based on the validation guide alone?

## Output format

For each finding:
- **Area:** which file or section
- **Severity:** Blocker (would prevent adoption) / Concern (would require mitigation before adoption) / Minor (worth noting)
- **Finding:** one sentence describing the trust or transparency gap
- **Recommendation:** what would resolve the concern

If an area demonstrates good transparency and responsible disclosure, say so in one line. Be direct — a library that runs VACUUM and REORG on production tables needs to earn trust explicitly, not assume it.

Working directory: `C:\Users\bradc\OneDrive\Desktop\Git Repos\delta-doctor`
```
