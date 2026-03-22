# coffee-market-rag

Initial scraper for ICO specialized report PDFs.

Usage:

```bash
python3 scripts/scrape_ico_specialized_reports.py
python3 scripts/scrape_ico_specialized_reports.py --insecure
python3 scripts/scrape_ico_specialized_reports.py --manifest-only
python3 -m unittest discover -s tests
```

Default outputs:

- `data/raw/ico/specialized-reports/reports.json`
- `data/raw/ico/specialized-reports/*.pdf` by default

Default scope:

- Only the `COFFEE MARKET REPORTS` section is scraped and downloaded
