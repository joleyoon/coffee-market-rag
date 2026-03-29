#!/usr/bin/env python3
"""Chunk extracted ICO report text into retrieval-ready records."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_utils import chunk_words, load_jsonl, write_jsonl
from scripts.pipeline_utils import build_date_metadata, extract_coffee_type_tags, extract_country_tags


DEFAULT_INPUT = Path("data/processed/ico/extracted_text/reports.jsonl")
DEFAULT_OUTPUT = Path("data/processed/ico/chunks/chunks.jsonl")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--chunk-size", type=int, default=400)
    parser.add_argument("--overlap", type=int, default=80)
    parser.add_argument("--dataset-version", default=None)
    return parser.parse_args()


def chunk_report_records(reports: list[dict], chunk_size: int, overlap: int, dataset_version: str | None = None) -> list[dict]:
    chunks: list[dict] = []

    for report in reports:
        chunk_counter = 0
        for page in report["pages"]:
            page_chunks = chunk_words(page["text"], chunk_size, overlap)
            for within_page_index, chunk_text in enumerate(page_chunks, start=1):
                chunk_counter += 1
                chunk_countries = extract_country_tags(chunk_text)
                chunk_coffee_types = extract_coffee_type_tags(f"{report['title']} {chunk_text}")
                chunks.append(
                    {
                        "report_id": report["report_id"],
                        "chunk_id": f"{report['report_id']}-p{page['page_number']:03d}-c{within_page_index:02d}",
                        "title": report["title"],
                        "source_url": report["source_url"],
                        "page_number": page["page_number"],
                        "chunk_index": chunk_counter,
                        "chunk_text": chunk_text,
                        "dataset_version": dataset_version or report.get("dataset_version"),
                        "country_tags": chunk_countries,
                        "coffee_type_tags": chunk_coffee_types,
                        "report_period": report.get("report_period", "historical"),
                        "ingest_status": report.get("ingest_status", "existing"),
                        **build_date_metadata(report.get("published_date")),
                    }
                )

        print(f"Chunked {report['report_id']} into {chunk_counter} chunks")

    return chunks


def write_chunk_records(chunks: list[dict], output_path: Path) -> None:
    write_jsonl(output_path, chunks)


def main() -> int:
    args = parse_args()
    reports = load_jsonl(Path(args.input_path))
    chunks = chunk_report_records(
        reports,
        chunk_size=args.chunk_size,
        overlap=args.overlap,
        dataset_version=args.dataset_version,
    )
    write_jsonl(Path(args.output_path), chunks)
    print(f"Wrote {len(chunks)} chunks to {args.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
