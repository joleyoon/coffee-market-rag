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
from scripts.pipeline_utils import (
    build_date_metadata,
    classify_report_period,
    extract_coffee_type_tags,
    extract_country_tags,
    iso_timestamp,
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
    parser.add_argument("--dataset-version", default=None)
    parser.add_argument("--ingested-at", default=None)
    return parser.parse_args()


def extract_reports(
    manifest_path: Path,
    output_dir: Path,
    jsonl_path: Path,
    dataset_version: str | None = None,
    ingested_at: str | None = None,
    previous_report_ids: set[str] | None = None,
) -> list[dict]:
    manifest = load_json(manifest_path)
    records: list[dict] = []
    ensure_directory(output_dir)
    resolved_ingested_at = ingested_at or iso_timestamp()

    for report in manifest["reports"]:
        local_path = report.get("local_path")
        if not local_path:
            continue

        pdf_path = Path(local_path)
        pages = extract_pages(pdf_path)
        full_text = clean_text("\n\n".join(page["text"] for page in pages))
        report_id = report_id_from_filename(report["filename"])
        published_date = parse_published_date(report["title"], report["filename"])
        date_metadata = build_date_metadata(published_date)
        combined_text = f"{report['title']} {full_text}"

        record = {
            "report_id": report_id,
            "title": report["title"],
            "filename": report["filename"],
            "source_url": report["url"],
            "local_pdf_path": str(pdf_path),
            "page_count": len(pages),
            "pages": pages,
            "full_text": full_text,
            "dataset_version": dataset_version,
            "ingested_at": resolved_ingested_at,
            "country_tags": extract_country_tags(full_text),
            "coffee_type_tags": extract_coffee_type_tags(combined_text),
            **date_metadata,
        }
        records.append(record)

    latest_published_date = max(
        (record["published_date"] for record in records if record.get("published_date")),
        default=None,
    )
    known_report_ids = previous_report_ids or set()

    for record in records:
        record["report_period"] = classify_report_period(record.get("published_date"), latest_published_date)
        record["ingest_status"] = "new" if record["report_id"] not in known_report_ids else "existing"
        write_json(output_dir / f"{record['report_id']}.json", record)
        print(f"Extracted {record['report_id']} ({record['page_count']} pages)")

    write_jsonl(jsonl_path, records)
    print(f"Wrote {len(records)} extracted reports to {jsonl_path}")
    return records


def main() -> int:
    args = parse_args()
    extract_reports(
        manifest_path=Path(args.manifest_path),
        output_dir=Path(args.output_dir),
        jsonl_path=Path(args.jsonl_path),
        dataset_version=args.dataset_version,
        ingested_at=args.ingested_at,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
