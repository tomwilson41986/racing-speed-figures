"""Day-after sectional ingestion, pars and rating uplifts.

Pipeline: Drive PDFs (drive_client) -> parse (parser) -> store (SQLite/S3) ->
match to live ratings (matching) -> pars (pars) -> uplift/explain -> day-after
email (report), all driven by cli.py.
"""
