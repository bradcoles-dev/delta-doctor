# Contributing to delta-doctor

Thank you for your interest in contributing. This project is an open-source Fabric Notebook Library for Delta table maintenance on Microsoft Fabric, and real-world feedback from practitioners using it in production is especially valuable.

AI tools are used in the development of this project. All domain decisions, design choices, and technical content are authored and reviewed by the maintainer.

---

## Ways to Contribute

- **Bug reports** — something behaves unexpectedly or produces incorrect results
- **Documentation improvements** — corrections, clarifications, or gaps in the reference docs
- **Feature requests** — capabilities you need that are missing from the library
- **Notebook contributions** — improvements to existing notebooks or new notebooks that fit the library's scope
- **Validation** — testing notebooks against different Fabric runtimes, Lakehouse configurations, or workload patterns and reporting results

---

## Reporting a Bug

Open a GitHub Issue and include:

1. Which notebook(s) are involved
2. What you expected to happen
3. What actually happened (output, error messages, unexpected behaviour)
4. Your Fabric Runtime version (Runtime 1.3 / Runtime 2.0)
5. Any relevant table properties or Spark session configurations in effect

The more context you provide, the faster the issue can be reproduced and addressed.

---

## Suggesting a Feature

Open a GitHub Issue describing:

1. The problem you are trying to solve
2. How you currently work around it (if at all)
3. What you would want the solution to look like

Check the [Roadmap](./README.md#roadmap) in the README first — your idea may already be planned.

---

## Making a Code Contribution

1. Fork the repository and create a branch from `main`
2. Make your changes
3. Test your changes against a real Fabric Lakehouse — unit tests cannot substitute for verifying notebook behaviour against actual Delta tables
4. Open a pull request with a clear description of what you changed and why

### Notebook conventions

- Follow the existing cell structure: markdown cell describing intent, then code cell
- Parameters cells must be tagged as `parameters` in the Fabric notebook UI (documented in the notebook markdown)
- Print meaningful output for every table-level decision (skipped / ran / error) — silent notebooks are hard to debug from pipeline run logs
- Never hardcode Lakehouse GUIDs, table names, or paths — everything user-specific must be a parameter

### Documentation conventions

- Reference docs live in `docs/` — update the relevant doc if your change affects documented behaviour
- Keep the `docs/medallion-recommendations.md` quick reference in sync with any setting changes — the source of truth is `LAYER_PROPERTIES` in `doctor_prevention_set_table_properties` and the baseline cell in `doctor_prevention_session_config`

---

## Development Environment

This library runs inside Microsoft Fabric. There is no local development environment — notebooks must be tested against a real Fabric workspace and Lakehouse.

1. Download the `.ipynb` files from `dist/` and import them into your Fabric workspace via **Import notebook** in the Data Engineering experience
2. Attach to a Lakehouse and run against real tables
3. Verify both the happy path (OPTIMIZE runs, VACUUM runs) and the skip path (healthy table, OPTIMIZE skipped correctly)

---

## Scripts

`scripts/` contains process tooling for maintainers — not runtime code.

| Script | Role | When to run |
|---|---|---|
| `cold-read-prompt.md` | Consistency auditor — finds bugs, contradictions, and broken invariants across all files | After large batches of changes; at release boundaries |
| `practitioner-review-prompt.md` | Skeptical practitioner — evaluates onboarding friction, unclear docs, and gaps between what is promised and delivered | Before any public release |
| `technical-review-prompt.md` | Senior Delta/Fabric engineer — stress-tests technical decisions, SQL operations, and Spark configurations for correctness | Before any public release or after changes to maintenance logic |
| `trust-review-prompt.md` | Cautious production adopter — evaluates transparency, risk disclosure, limitations, and production safety | Before any public release or major version bump |

---

## Questions

Open a GitHub Discussion or Issue. For direct contact, reach out via [LinkedIn](https://www.linkedin.com/in/brad-coles/).
