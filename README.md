# coffee-market-rag

Initial scraper for ICO specialized report PDFs.

Usage:

```bash
python3 scripts/scrape_ico_specialized_reports.py
python3 scripts/scrape_ico_specialized_reports.py --download
python3 scripts/scrape_ico_specialized_reports.py --download --insecure
python3 -m unittest discover -s tests
```

Default outputs:

- `data/raw/ico/specialized-reports/reports.json`
- `data/raw/ico/specialized-reports/*.pdf` when `--download` is enabled
