# API-TASKS.md — Tasks deferred to the `observatory-api` repository

Working through [AUDIT-2026-08-18.md](AUDIT-2026-08-18.md) in this (`observatory-pipeline`)
repository surfaces changes that cannot be made here at all, because they belong to the
separate `observatory-api` (CodeIgniter 4 / PHP) repository — a DB migration, an endpoint
contract change, a new wire field, or new server-side validation.

Those are collected here rather than silently skipped. Nothing in this file has been
implemented; it is a queue to be picked up in `observatory-api` later.

Each entry records:
- **Origin** — the audit finding (or other work) that produced it
- **What** — the change required on the API side
- **Why** — what in the pipeline is blocked or degraded without it
- **Pipeline side** — what, if anything, was already done here in the meantime

---

_(no entries yet)_
