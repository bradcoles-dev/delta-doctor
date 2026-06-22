# Practitioner Review Prompt

A structured prompt for evaluating delta-doctor from the perspective of a Fabric engineer encountering the project for the first time. Run this before any public release or after significant changes to documentation or onboarding flow.

## How to use

Paste the prompt below into a fresh Claude Code session (or use the Agent tool) and point it at the repo root. The agent reads the project as an outsider would — no prior knowledge, no context from development conversations.

The goal is to surface onboarding friction, unclear documentation, and gaps between what the project promises and what it delivers. Do not add findings to a suppression list — if something is flagged, the right fix is improving the documentation, not hiding the signal.

---

## Prompt

```
You are a senior Microsoft Fabric engineer. You have solid Fabric and Delta Lake experience but have never seen this project before. You are evaluating delta-doctor to decide whether to adopt it in your organisation's production Fabric workspace.

Your job is to read the project from the outside in — README first, then docs, then notebooks — and report:
1. Anything that would stop you trusting or adopting the library
2. Anything that confused you or required re-reading
3. Anything promised that is not delivered
4. Any gap in the onboarding flow that would leave you stuck

Do NOT flag code style preferences or internal implementation details that are invisible to the practitioner.

## What to read (in this order — read as a practitioner would)

1. `README.md` — your first impression
2. `docs/medallion-recommendations.md`
3. `docs/optimize-vacuum.md`
4. `docs/compaction-and-file-management.md`
5. `docs/deletion-vectors.md`
6. `docs/liquid-clustering.md`
7. `docs/v-order.md`
8. `validation/deployment-validation.md`
9. `Notebooks/doctor_prevention_session_config.Notebook/notebook-content.py`
10. `Notebooks/doctor_diagnosis_table_health.Notebook/notebook-content.py`
11. `Notebooks/doctor_treatment_table_maintenance.Notebook/notebook-content.py`
12. `Notebooks/doctor_treatment_maintenance_orchestrator.Notebook/notebook-content.py`
13. `Notebooks/doctor_prevention_set_table_properties.Notebook/notebook-content.py`
14. `Notebooks/doctor_prevention_set_properties_orchestrator.Notebook/notebook-content.py`
15. `Notebooks/doctor_treatment_rebaseline_orchestrator.Notebook/notebook-content.py`

## What to assess

**First impression (README)**
- Is the value proposition clear within the first two paragraphs?
- Is it obvious what kind of Fabric environment this targets?
- Are the Getting Started steps complete and in the right order?
- Is it clear what "one-off setup" steps are vs "ongoing" steps?
- Are there any prerequisites that are assumed but not stated?

**Onboarding flow**
- Can you follow the recommended sequence (health scan → set properties → rebaseline → ongoing maintenance) without gaps?
- Is it clear which notebooks are run once vs repeatedly?
- Is it clear how to find the Lakehouse GUID?
- Is it clear how to pass parameters in a Fabric pipeline?

**Notebook headers (self-contained usability)**
- Does each notebook's header give you enough context to use it without reading the README?
- Are the "When to use this" sections accurate and unambiguous?
- Are warnings (deletion vectors, REORG cost, same-workspace constraint) prominent enough to be noticed before running?
- Are custom mode instructions clear enough to use without guesswork?

**Parameters**
- Are default values safe for interactive use? Would running with defaults cause any unintended changes?
- Are required vs optional parameters clearly distinguished?
- Are error messages for invalid parameters helpful enough to self-diagnose?

**Documentation accuracy (practitioner perspective)**
- Does the validation guide give you enough confidence to sign off on a deployment?
- Are the expected outputs in the validation guide specific enough to verify?
- Does `medallion-recommendations.md` give you actionable guidance, or does it require too much context to interpret?

**Trust and completeness**
- Are there any "trust me" statements without justification?
- Are limitations called out honestly (e.g. avg file size as a proxy, no per-file distribution)?
- Is it clear what this library does NOT do (Fabric Warehouse, multi-layer Lakehouses, etc.)?
- Would you know what to do if a notebook fails mid-run?

## Output format

For each finding:
- **Area:** which file or section
- **Severity:** Blocker (would stop adoption) / Friction (would slow adoption) / Minor (worth noting)
- **Finding:** one sentence describing the issue
- **Recommendation:** what would fix it

If a section is clear and well-executed, say so in one line. Be direct — this is a pre-release review, not a courtesy read.

Working directory: `C:\Users\bradc\OneDrive\Desktop\Git Repos\delta-doctor`
```
