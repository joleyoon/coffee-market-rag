#!/usr/bin/env python3
"""Export chunk data for the static GitHub Pages chatbot."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_utils import ensure_directory, load_jsonl, write_json


DEFAULT_INPUT = Path("data/processed/ico/chunks/chunks.jsonl")
DEFAULT_OUTPUT = Path("docs/data/search-data.json")
DEFAULT_TREND_INPUT = Path("data/processed/ico/trends/trend-data.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--trend-path", default=str(DEFAULT_TREND_INPUT))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_path)
    output_path = Path(args.output_path)
    trend_path = Path(args.trend_path)
    chunks = load_jsonl(input_path)

    payload = {
        "chunk_count": len(chunks),
        "report_count": len({chunk["report_id"] for chunk in chunks}),
        "chunks": [
            {
                "report_id": chunk["report_id"],
                "chunk_id": chunk["chunk_id"],
                "title": chunk["title"],
                "source_url": chunk["source_url"],
                "published_date": chunk["published_date"],
                "page_number": chunk["page_number"],
                "chunk_text": chunk["chunk_text"],
            }
            for chunk in chunks
        ],
    }
    if trend_path.exists():
        import json

        payload["trend_data"] = json.loads(trend_path.read_text(encoding="utf-8"))

    ensure_directory(output_path.parent)
    write_json(output_path, payload)
    print(f"Wrote static search data to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
