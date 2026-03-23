# coffee-market-rag

Initial scraper for ICO specialized report PDFs.

Usage:

```bash
python3 scripts/scrape_ico_specialized_reports.py
python3 scripts/scrape_ico_specialized_reports.py --insecure
python3 scripts/scrape_ico_specialized_reports.py --manifest-only
python3 scripts/extract_report_text.py
python3 scripts/chunk_reports.py
python3 scripts/build_vector_index.py
python3 scripts/query_index.py "global coffee demand"
python3 app/app.py "arabica supply risk"
python3 app/app.py --serve
python3 -m unittest discover -s tests
```

Default outputs:

- `data/raw/ico/specialized-reports/reports.json`
- `data/raw/ico/specialized-reports/*.pdf` by default

Default scope:

- Only the `COFFEE MARKET REPORTS` section is scraped and downloaded

Pipeline outputs:

- `data/processed/ico/extracted_text/reports.jsonl`
- `data/processed/ico/extracted_text/*.json`
- `data/processed/ico/chunks/chunks.jsonl`
- `data/processed/ico/index/tfidf_index.pkl`

Web app:

- Run `python3 app/app.py --serve`
- Open `http://127.0.0.1:8000`
