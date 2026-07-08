# Timeform TFig reconciliation

A daily pipeline that checks our UK/IRE speed figures against Timeform's
published time figures (TFigs) for the **previous day's** racing, emails a
comparison report, and maintains a guarded rolling correction that can
(optionally) be fed back into the live figures.

The model is already calibrated against Timeform's historical `timefigure`
(r≈0.925, MAE≈6.7 lb on ~600k runners — see
`docs/ratings_accuracy_review_2026-07.md`). This pipeline adds the *forward*
check: how well yesterday's live figures actually tracked Timeform, day by day.

## What runs

`.github/workflows/timeform_recon.yml` runs at **10:00 UTC** each morning (late
enough for Timeform to finish publishing the previous day's TFigs; still well
before the 21:00 combined live email). It:

1. logs into Timeform and loads `results/yesterday`;
2. parses each meeting's runners + TFigs (preferring a JSON feed if the site
   exposes one, falling back to HTML selectors);
3. matches them to our figures for that date
   (`output/uk_audit/<date>/audit_full_<date>.csv`) using the shared
   `src/sectionals/matching.py` name/track join;
4. writes the reconciliation and **emails a report**;
5. recomputes the rolling correction.

## Outputs (committed under `output/timeform_recon/`)

| File | Contents |
|---|---|
| `history.csv` | one row per day: `n_matched, corr, mae, bias, within_10, n_outliers` |
| `<date>.csv` | per matched runner: `our_fig, tfig, diff, outlier` |
| `<date>.md` / `<date>.html` | the rendered comparison report |
| `correction.json` | current rolling bias/scale + whether it would apply |
| `raw/<date>/` | dumped HTML/screenshots/JSON — **gitignored**, uploaded as a CI artifact |

`bias` is `ours − Timeform`: **positive means we rate higher than Timeform.**

## First-time setup

1. Add two GitHub repository secrets (Settings → Secrets and variables →
   Actions): **`TIMEFORM_USER`** and **`TIMEFORM_PASS`**. `SMTP_USER` / `SMTP_PASS`
   already exist for the daily emails.
2. Run the workflow once manually (Actions → *Timeform TFig Reconciliation* →
   *Run workflow*) with **`capture_only = true`**. Download the
   `timeform-raw-<run_id>` artifact — it contains the real logged-in HTML/JSON.
   The parser selectors in `src/timeform/results.py` were written defensively but
   are finalised against that capture (fixtures live in
   `tests/timeform/fixtures/`).
3. Once the parser matches a good share of runners, let the daily schedule run.

## The correction (report-only by default)

`src/timeform/correction.py` pools the trailing 14 days of `history.csv` into a
bias estimate. It sets `apply: true` in `correction.json` **only if every
guardrail passes**: ≥7 days present, ≥200 matched runners, `|bias|` > 3 lb;
the applied bias is then clipped to ±6 lb.

Even when `apply: true`, the live pipeline does **nothing** unless you also set
the master switch. To turn the feedback loop on:

- set repo/workflow env **`TIMEFORM_CORRECTION=1`** for the live ratings run.

`src/live_ratings.py::_apply_timeform_correction` then subtracts the bias (and
applies any scale) from `figure_calibrated` as the last pipeline step. It is
fully reversible — unset the env var or set `"apply": false` in
`correction.json`. Start report-only, watch the daily emails for a couple of
weeks, and only enable auto-correction once the matching and bias look stable.

## Running it by hand

```bash
# Dump the real page structure (needs network + creds; runs in CI):
python -m src.timeform.cli capture-html --date yesterday

# Full run: fetch, reconcile, store, email:
python -m src.timeform.cli run --date 2026-07-07

# Reprocess an already-captured day offline (no network), no email:
python -m src.timeform.cli run --date 2026-07-07 --from-raw --no-email

# Offline unit tests (no network):
python -m pytest tests/timeform/ -q
```

## Design note — why the scraper is defensive

`timeform.com` is unreachable from the dev sandbox (egress policy), so the
network layer (`auth.py`, `client.py`) can only be exercised in CI. All
parsing/matching/scoring (`results.py`, `reconcile.py`, `correction.py`,
`store.py`) is pure and fully unit-tested offline; the client dumps all raw
material to disk on every run so selectors can be refined from a real capture
rather than guessed.
