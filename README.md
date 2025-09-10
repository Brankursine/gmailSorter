# Gmail Auto-Labeler by Domain (`hosts/*`) + Consistency Audit

**Version V9.1 — UNPROCESSED runner + audits + split-by-email + verbose + deep-scan helpers**

This Google Apps Script organizes your Gmail automatically by **sender domain** (e.g., `hosts/google.com`) and, for selected domains, by **full email address** (e.g., `hosts/reltio.com/john.doe@reltio.com`). It also creates/maintains **Gmail filters** so future mail is labeled on arrival, performs **retro-apply** to old threads, and includes several **audits** to keep labeling consistent over time.

---

## Quick Start

1. **Create a new Apps Script** at [https://script.google.com/](https://script.google.com/) and paste the code.
2. **Enable the Advanced Gmail Service**: *Services (puzzle icon) → “Gmail API” → Add/Enable*.
3. (Optional) **Adjust settings** at the top of the script (labels, thresholds, time budget, etc.).
4. **Run** `runProcessUnprocessedAndReport()` once.
5. **Check the report email** sent to your account for results and diagnostics.
6. (Optional) Set a **time-based trigger** (e.g., hourly) for automatic maintenance.

---

## What It Does

* **Labels incoming mail** by domain under a parent label (default `hosts`):

  * **Normal domains**: `hosts/<domain>`
  * **Split domains** (per address): `hosts/<domain>/<email>`
  * **Rare/low-traffic senders**: `hosts/_rare_`
* **Creates Gmail filters** so new mail is labeled on arrival (`from:@domain` or `from:email`).
* **Retro-applies** labels to existing threads that match a domain/address.
* **Auto-migrates** threads from `_rare_` → normal domain/email once the threshold is met.
* **Audits & fixes** consistency issues (e.g., missing parent label, missing filters).
* **Sends an HTML report** after each run with metrics, top domains, and decision traces.

---

## Key Concepts & Labels

* **Parent label**: `hosts`
  All domain/email labels live under this branch. The script ensures parents are present.
* **Domain label**: `hosts/<domain>` (e.g., `hosts/google.com`)
* **Per-email label (split domains)**: `hosts/<domain>/<email>`
  Used for domains listed in `SPLIT_BY_FULL_EMAIL_DOMAINS`.
* **Rare bucket**: `hosts/_rare_`
  Temporary holding for senders below threshold; migrated automatically later.

---

## Default Configuration (edit at the top of the script)

* `PARENT_LABEL = 'hosts'` — Root label branch.
* `MIN_FILTER_THRESHOLD = 1` — Create domain/email label & filter at ≥ N threads.
* `ONLY_UNREAD = false` — Process all mail (set `true` to test on unread only).
* `ARCHIVE_NEW = false` — If `true`, new matching mail is auto-archived (removes INBOX).
* **Performance & safety**:

  * `PAGE_SIZE = 200`, `TIME_BUDGET_MS = 5*60*1000`, `TIME_STOP_MARGIN_MS = 10*1000`,
  * `MAX_THREADS_PER_RUN = 10000` (hard guard).
* **Split by full email** (per-address labels):

  ```js
  const SPLIT_BY_FULL_EMAIL_DOMAINS = new Set([
    'gmail.com','bk.ru','mail.ru','yandex.ru','hotmail.com','reltio.com','getcourse.ru'
  ]);
  ```
* **Retro-apply** when creating filters:
  `APPLY_FILTER_RETROACTIVE_ON_CREATE = true` (respecting run’s time budget).
* **Caches & checkpoints** stored in **User Properties** by default; set
  `USE_SCRIPT_PROPERTIES = true` if you want **Script Properties** instead.

> Tip: Start with `ONLY_UNREAD = true` and a small `TIME_BUDGET_MS` for a safe dry run.

---

## How It Works (step-by-step)

1. **Runner**: `runProcessUnprocessedAndReport()`

   * Builds a base query: `-label:hosts` (and `is:unread` if configured).
   * Iterates threads in batches within a **time budget**.
2. **Fast identity detection**:

   * Reads message metadata via `Gmail.Users.Threads.get(..., format: 'metadata')` to determine **best incoming sender** (prefers true incoming over your own sent copies).
   * Extracts **email** and **domain** (robust RFC-like parsing, tolerates `Return-Path`, `Sender`, etc.).
3. **Labeling decision**:

   * If domain is in `SPLIT_BY_FULL_EMAIL_DOMAINS`: count per **email**; else, per **domain**.
   * If count ≥ threshold → **create/ensure** label & filter; **retro-apply** (time-boxed).
   * If below threshold → apply `hosts/_rare_`.
   * **Always** attach the parent `hosts` label.
4. **Consistency audits** (opportunistic during processing):

   * If a thread already has `hosts/<...>` child but no parent `hosts`, parent is added.
   * Domain/email retro-apply and `_rare_` → normal migrations are triggered.
5. **Reporting**:

   * Sends an **HTML report** with totals, timing, top domains, and (optionally) a **decision trace** for up to `DEBUG_MAX_ROWS`.

> **Deep scan helper** exists (`getBestIncomingIdentityForThreadDeep_`) for full-message inspection. By default the fast metadata path is used. If you want a fallback, add a conditional call to the deep version when the fast path can’t identify a sender.

---

## Main Tasks & When to Use Them

* **Daily driver**

  * `runProcessUnprocessedAndReport()` — label new/unlabeled threads, maintain filters, report.

* **Audits / maintenance**

  * `ensureFiltersForAllHostLabels()` — ensure every `hosts/*` label has a matching Gmail filter.
  * `retroApplyAllHostLabels()` — apply existing `hosts/*` labels to past threads.
  * `removeRareWhereNormalPresent()` — remove `_rare_` where a normal label exists.
  * `auditAndFixHostsParent()` (alias: `backfillParentHostsLabel()`) — add missing parent `hosts` where children exist.
  * **Split-domain focused**

    * `auditSplitDiscoverAndCreateForDomain(domain, maxCreatePerRun)` — scan `hosts/<domain>` threads, discover frequent **emails**, create `hosts/<domain>/<email>` labels & filters, retro-apply.
    * `auditSplitDiscoverAndCreateAll(maxCreatePerDomain)` — do the above for all split domains.
    * `auditSplitExistingEmailLabels()` — ensure filters & retro-apply for existing email labels.

* **Reset / cleanup**

  * `wipeHostsEverything()` — **dangerous**: deletes hosts-filters, removes hosts-labels from threads, deletes labels, clears caches/checkpoints.
  * `resetCheckpoints()`, `resetDomainCache()`, `resetAllNamespaceProps()` — targeted resets.

---

## Gmail Filters & Retro-Apply

* For domains: criteria `from: @<domain>` (e.g., `@example.com`) → add `hosts/<domain>`.
* For split domains: criteria `from: <email>` (quoted exact match) → add `hosts/<domain>/<email>`.
* **Retro-apply**: on filter creation (if enabled) and via audit helpers—time-boxed to avoid timeouts.

> **Note**: Gmail filters can add **one** label per filter. The script adds the **parent** label during processing. That’s why audits include “add missing parent” routines.

---

## Reporting (email)

* **Subject**: `Gmail hosts sweep [PHASE] — YYYY-MM-DD HH:MM (threads N)`
* Includes:

  * Start/end times (local + UTC), duration, phase, window (if used), query hint
  * Threads processed, messages scanned, labeled rare/normal, new labels/filters, errors
  * Retro-apply counts, `_rare_` migrations, parent fixes, audits triggered
  * **Top domains** this run
  * **Diagnostics table** (optional, gated by `DEBUG_DECISIONS` & focus filters)

Recipient: `Session.getActiveUser().getEmail()` (override by editing `REPORT_RECIPIENT` if needed).

---

## Performance & Quotas

* Operates under Apps Script & Gmail API quotas.
* Uses a **time budget** (`TIME_BUDGET_MS`) with a **stop margin** to finish cleanly.
* Batches retro-apply operations (`RETRO_APPLY_BATCH`) and sleeps between chunks to reduce throttle.
* Maintains **caches** (User/Script Properties) for domain/address counts to avoid repeated counting.

> If you hit limits (timeouts, rate limits), reduce `PAGE_SIZE`, lower retro-apply batch sizes, or shorten the time budget.

---

## Safety Controls & Testing

* Set `ONLY_UNREAD = true` for a safe “preview” run.
* Consider raising `MIN_FILTER_THRESHOLD` to avoid creating labels/filters too eagerly.
* Use `DEBUG_FOCUS_DOMAINS`/`DEBUG_FOCUS_EMAILS` to narrow diagnostic logs.
* Run `wipeHostsEverything()` only if you truly want to reset the system.

---

## Known Limitations / Notes

* **Filters limit**: Gmail caps the number of filters (commonly \~1,000). Excessive per-email labels may hit that limit.
* **Label nesting depth**: While deep nesting works, keep branches tidy to avoid clutter.
* **Parent label** must be maintained by the script (filters can only add one label).
* Identity parsing is robust, but edge-case sender headers can still be tricky; the deep helper is available if needed.

---

## Recommended Workflow

1. **Tune settings**: parent label, thresholds, split domains.
2. **Dry run**: `ONLY_UNREAD = true`, reduced time budget.
3. **Inspect report**: check top domains, labels/filters created, and diagnostics.
4. **Go live**: `ONLY_UNREAD = false`, normal time budget.
5. **Periodic maintenance** (optional): schedule

   * weekly `ensureFiltersForAllHostLabels()` and `retroApplyAllHostLabels()`,
   * monthly `auditSplitDiscoverAndCreateAll()` for split domains.

---

## Design Rationale (why it’s built this way)

* **Fast metadata path** keeps runs under quota while correctly attributing incoming identities.
* **Split-by-email** for noisy consumer/business domains avoids giant “catch-all” domain buckets.
* **Rare bucket** prevents label/filter sprawl for one-off senders, with automatic migration later.
* **Audits** fix inevitable drift (missing parent labels, old threads unlabeled, etc.).
* **Time-budgeted loops** + **caches** ensure runs are repeatable and safe for large mailboxes.

---

## Extending or Tweaking

* **Enable deep fallback** when identity is missing:
  Inside `processSingleThread_`, if `!domain || !email`, call `getBestIncomingIdentityForThreadDeep_(thread)` and retry the decision.
* **Custom ignore list**: add domains to `IGNORE_DOMAINS`.
* **Archiving**: set `ARCHIVE_NEW = true` to auto-file matching mail out of the inbox.
* **Different parent**: change `PARENT_LABEL` to maintain a separate taxonomy (e.g., `senders`).

---

## Troubleshooting

* **Threads not being labeled**

  * Check that the runner query `-label:hosts` actually finds unlabeled mail.
  * Verify the Advanced Gmail Service is enabled.
  * Inspect report diagnostics (enable `DEBUG_DECISIONS`).
* **Parent label missing**

  * Run `auditAndFixHostsParent()` (aka `backfillParentHostsLabel()`).
* **Filters exist but old mail is unlabeled**

  * Ensure `APPLY_FILTER_RETROACTIVE_ON_CREATE = true` or run `retroApplyAllHostLabels()`.
* **Too many per-email labels**

  * Reduce `SPLIT_BY_FULL_EMAIL_DOMAINS` or raise thresholds.

---

## Function Index (for reference)

* **Runner**: `runProcessUnprocessedAndReport()`
* **Audits**:
  `ensureFiltersForAllHostLabels()`, `retroApplyAllHostLabels()`,
  `removeRareWhereNormalPresent()`, `auditAndFixHostsParent()`,
  `auditSplitDiscoverAndCreateForDomain()`, `auditSplitDiscoverAndCreateAll()`,
  `auditSplitExistingEmailLabels()`
* **Utilities**: label/filter ensure & retro-apply helpers, identity parsing, caches
* **Resets**: `wipeHostsEverything()`, `resetCheckpoints()`, `resetDomainCache()`, `resetAllNamespaceProps()`

---

## Practical Action Plan

1. Paste the script → enable Gmail API (Advanced Service).
2. Set `PARENT_LABEL`, `MIN_FILTER_THRESHOLD`, and `SPLIT_BY_FULL_EMAIL_DOMAINS`.
3. Dry-run with `ONLY_UNREAD = true`; run `runProcessUnprocessedAndReport()`.
4. Review the report → adjust thresholds/splits.
5. Go live (`ONLY_UNREAD = false`) and add a time-based trigger.
6. Schedule monthly audits for housekeeping.

---

## Alternative Approaches You Might Consider

* **Pure domain labels only** (no split): set `SPLIT_BY_FULL_EMAIL_DOMAINS = new Set([])`.
* **Project-based taxonomy**: add a second, independent labeling branch (e.g., `projects/*`) via extra queries.
* **BigQuery/Sheets reporting**: log per-run stats to a Sheet or export JSON via Properties for analytics.

---

## Version

* **V9.1** — UNPROCESSED runner with audits, split-by-email support, verbose diagnostics, time-budgeted retro-apply, caches/checkpoints, rare→normal migrations, and HTML email reports.

---

### How I arrived at this README (brief)

* I mapped each constant/major function to user-facing behaviors (label structure, filters, retro-apply, audits, resets).
* I organized the doc around setup → behavior → operations → audits → safety/troubleshooting.
* I highlighted tuning knobs (thresholds, split domains, time budget) and provided a safe rollout path.

If you want this saved as a `.md` file or tailored for a GitHub repository (with badges, CI hints, or a minimal “Examples” section), say the word and I’ll package it accordingly.
