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
enough for the previous day's PDFs to be saved to Drive; still well before the
21:00 combined live email). It:

1. reads the previous day's Timeform **"Race Result" PDFs from Google Drive**
   (`<root>/DD.MM.YY/<Track>/Race Result HH_MM COURSE Weekday DD Month.pdf`);
2. parses each PDF's runner table by font/coordinate clustering
   (`src/timeform/tfig_pdf.py`) to recover the TFR and **Tfig** columns;
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
| `raw/<date>/` | legacy scrape dump — **gitignored**, uploaded as a CI artifact |

`bias` is `ours − Timeform`: **positive means we rate higher than Timeform.**

## Where the TFigs come from (and why not the website)

`timeform.com`'s sign-in form is behind an **image CAPTCHA**, and the site sits
behind an Azure Front Door WAF that challenges datacenter IPs. An automated
login cannot complete from CI, so the scrape never ran successfully.

The account owner already saves the Timeform result PDFs to Google Drive daily,
into the same folder the sectionals pipeline reads
(`SECTIONALS_DRIVE_FOLDER_ID`, default `1tR8_Hhq3vBAuohj48fYtVT8arY8WlsDr`),
laid out as `<root>/DD.MM.YY/<Track>/`. Those PDFs carry the TFR and Tfig
columns, so `src/timeform/drive_source.py` replaces the scrape.

Three unrelated document families share each track folder and **only the first
is Timeform**:

| filename shape | source | used? |
|---|---|---|
| `Race Result HH_MM COURSE Weekday DD Month.pdf` | Timeform | **yes** |
| `<Course> Racing Results _ <date> HH_MM.pdf` | Racing TV | no (no Tfig) |
| `HH_MM _ <Course> _ <date> _ At The Races*.pdf` | At The Races | no (`src/sectionals`) |

`drive_source.is_timeform_result_pdf()` is the filter; the accept/reject list is
pinned by `tests/timeform/test_drive_source.py`.

## First-time setup

1. The workflow needs one secret: **`GDRIVE_SA_JSON`** — the same
   service-account JSON `sectional_ratings.yml` uses. Share the Drive folder
   read-only with the service-account email. `SMTP_USER` / `SMTP_PASS` already
   exist for the daily emails.
2. Check what the source sees for a date:
   `python -m src.timeform.cli list-pdfs --date 2026-08-03`.
3. Let the daily schedule run.

The legacy scrape path is still selectable (`--source scrape`, or the
`source` input on *Run workflow*); it installs Chromium + xvfb on demand and is
not expected to work. `TIMEFORM_USER` / `TIMEFORM_PASS` / `TIMEFORM_COOKIE` /
`PW_PROXY` are only read on that path.

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
# Which Drive PDFs would be used for a date (needs GDRIVE_SA_JSON):
python -m src.timeform.cli list-pdfs --date 2026-08-03

# Full run from Drive: fetch, parse, reconcile, store, email:
python -m src.timeform.cli run --date 2026-08-03

# Same, without sending the email:
python -m src.timeform.cli run --date 2026-08-03 --no-email

# Legacy scrape (dead — image CAPTCHA) and legacy raw-dump replay:
python -m src.timeform.cli run --date 2026-07-07 --source scrape
python -m src.timeform.cli run --date 2026-07-07 --source raw --no-email

# Offline unit tests (no network):
python -m pytest tests/timeform/ -q
```

`--source` is `drive` (default) | `scrape` | `raw`. `--from-raw` still works as
a deprecated alias for `--source raw`.

## Design note — parsing the PDFs

`page.extract_text()` interleaves the Timeform result table's columns (horse
name and pedigree overlap) and is not reliably parseable, and the x-coordinates
are content-driven so nothing can be keyed off a fixed column position. So
`src/timeform/tfig_pdf.py` works from `extract_words(use_text_flow=True)` with
font names and sizes: runners are anchored on the bold size-3.00 name tokens,
and the TFR/Tfig pair is separated by clustering x-centres and cross-checking
against the `TFR` header token — never by magnitude, because TFR < Tfig
genuinely occurs.

Everything downstream of the source (`reconcile.py`, `correction.py`,
`store.py`, `report.py`) is source-agnostic and fully unit-tested offline. A
single unparseable PDF is logged and skipped rather than aborting the day; a day
with **no** Timeform PDFs fails the run loudly instead of writing a hollow
zero-match row into `history.csv`.
