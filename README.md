# coffee-market-rag

Coffee market RAG pipeline for ICO Coffee Market Reports.

The repo now follows a real data pipeline shape:

`ingest -> clean -> chunk -> embed -> store -> serve`

What is included:

- Scheduled ingestion with a local Python scheduler and a GitHub Actions cron workflow
- Versioned dataset snapshots under `data/processed/ico/versions/<dataset_version>/`
- Metadata tagging on reports and chunks for `country`, `coffee_type`, and `date`
- Backward-compatible latest aliases for the CLI app and local web app

## Commands

```bash
python3 scripts/scrape_ico_specialized_reports.py
python3 scripts/extract_report_text.py
python3 scripts/chunk_reports.py
python3 scripts/build_vector_index.py
python3 scripts/run_data_pipeline.py
python3 scripts/run_data_pipeline.py --skip-ingest
python3 scripts/schedule_data_pipeline.py --run-once --skip-ingest
python3 scripts/schedule_data_pipeline.py --daily-at 06:30
python3 scripts/query_index.py "global coffee demand"
python3 scripts/query_index.py --country Brazil --coffee-type Arabica "supply outlook"
python3 app/app.py "arabica supply risk"
python3 app/app.py --serve
python3 -m unittest discover -s tests -v
```

## Pipeline Outputs

Latest aliases:

- `data/processed/ico/extracted_text/reports.jsonl`
- `data/processed/ico/chunks/chunks.jsonl`
- `data/processed/ico/index/tfidf_index.pkl`
- `data/processed/ico/trends/trend-data.json`
- `data/processed/ico/pipeline_manifest.json`

Immutable snapshot outputs:

- `data/processed/ico/versions/<dataset_version>/extracted_text/reports.jsonl`
- `data/processed/ico/versions/<dataset_version>/chunks/chunks.jsonl`
- `data/processed/ico/versions/<dataset_version>/index/tfidf_index.pkl`
- `data/processed/ico/versions/<dataset_version>/trends/trend-data.json`
- `data/processed/ico/versions/<dataset_version>/pipeline_manifest.json`

Raw inputs:

- `data/raw/ico/specialized-reports/reports.json`
- `data/raw/ico/specialized-reports/*.pdf`

## Metadata

Each extracted report and retrieval chunk is enriched with:

- `country_tags`
- `coffee_type_tags`
- `published_date`
- `report_month`
- `report_year`
- `dataset_version`
- `report_period` (`latest` vs `historical`)
- `ingest_status` (`new` vs `existing`)

## Search and Serve

Metadata-aware retrieval is available from both the CLI and app:

- `--country`
- `--coffee-type`
- `--published-after`
- `--published-before`
- `--dataset-version`

Run the local app with:

```bash
python3 app/app.py --serve
```

Then open `http://127.0.0.1:8000`.

## Automation

CI:

- `.github/workflows/ci.yml` runs on every push and pull request to `main`
- It runs the unit tests
- It smoke-tests the versioned pipeline runner
- It smoke-tests the app against the generated latest index

Scheduled refresh:

- `.github/workflows/data-pipeline.yml` runs on a daily cron and on manual dispatch
- It refreshes the latest dataset snapshot and uploads the processed artifacts
