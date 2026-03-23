#!/usr/bin/env python3
"""Extract text from downloaded ICO PDFs into structured JSONL records."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PyPDF2 import PdfReader

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_utils import (
    clean_text,
    ensure_directory,
    load_json,
    parse_published_date,
    report_id_from_filename,
    write_json,
    write_jsonl,
)


DEFAULT_MANIFEST = Path("data/raw/ico/specialized-reports/reports.json")
DEFAULT_OUTPUT_DIR = Path("data/processed/ico/extracted_text")
DEFAULT_JSONL = DEFAULT_OUTPUT_DIR / "reports.jsonl"


def extract_pages(pdf_path: Path) -> list[dict]:
    reader = PdfReader(str(pdf_path))
    pages: list[dict] = []
    for index, page in enumerate(reader.pages, start=1):
        page_text = clean_text(page.extract_text() or "")
        if page_text:
            pages.append({"page_number": index, "text": page_text})
    return pages


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-path", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--jsonl-path", default=str(DEFAULT_JSONL))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest_path)
    output_dir = Path(args.output_dir)
    jsonl_path = Path(args.jsonl_path)

    manifest = load_json(manifest_path)
    records: list[dict] = []
    ensure_directory(output_dir)

    for report in manifest["reports"]:
        local_path = report.get("local_path")
        if not local_path:
            continue

        pdf_path = Path(local_path)
        pages = extract_pages(pdf_path)
        full_text = clean_text("\n\n".join(page["text"] for page in pages))
        report_id = report_id_from_filename(report["filename"])
        published_date = parse_published_date(report["title"], report["filename"])

        record = {
            "report_id": report_id,
            "title": report["title"],
            "filename": report["filename"],
            "source_url": report["url"],
            "published_date": published_date,
            "local_pdf_path": str(pdf_path),
            "page_count": len(pages),
            "pages": pages,
            "full_text": full_text,
        }
        records.append(record)

        write_json(output_dir / f"{report_id}.json", record)
        print(f"Extracted {report_id} ({len(pages)} pages)")

    write_jsonl(jsonl_path, records)
    print(f"Wrote {len(records)} extracted reports to {jsonl_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
