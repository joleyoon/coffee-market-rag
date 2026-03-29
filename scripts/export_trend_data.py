#!/usr/bin/env python3
"""Extract reusable trend-series data from the latest ICO report."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_utils import load_json, write_json


DEFAULT_INPUT_DIR = Path("data/processed/ico/extracted_text")
DEFAULT_OUTPUT = Path("data/processed/ico/trends/trend-data.json")
MONTH_MAP = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
SERIES_CONFIG = [
    ("ico_composite", "ICO Composite", "#82c694"),
    ("colombian_milds", "Colombian Milds", "#f0bf78"),
    ("other_milds", "Other Milds", "#f5e7c3"),
    ("brazilian_naturals", "Brazilian Naturals", "#86bbf0"),
    ("robustas", "Robustas", "#d97852"),
    ("new_york", "New York", "#c4a1ff"),
    ("london", "London", "#9dd3c8"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


def parse_month_label(label: str) -> str:
    month_text, year_text = label.split("-")
    month = MONTH_MAP[month_text]
    year = 2000 + int(year_text)
    return f"{year:04d}-{month:02d}-01"


def latest_report(input_dir: Path) -> dict:
    reports = [load_json(path) for path in input_dir.glob("cmr-*.json")]
    if not reports:
        raise FileNotFoundError(f"No extracted report JSON files found in {input_dir}")
    return max(reports, key=lambda report: report.get("published_date") or "")


def extract_table_block(full_text: str, table_name: str, next_table_name: str) -> str:
    try:
        after_start = full_text.split(table_name, 1)[1]
        return after_start.split(next_table_name, 1)[0]
    except IndexError as exc:
        raise ValueError(f"Could not locate {table_name}") from exc


def parse_monthly_price_rows(table_block: str) -> list[tuple[str, list[float]]]:
    pattern = re.compile(
        r"([A-Z][a-z]{2}-\d{2})\s+"
        r"(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)"
    )
    rows = []
    for match in pattern.finditer(table_block):
        label = match.group(1)
        values = [float(match.group(index)) for index in range(2, 9)]
        rows.append((label, values))
    if not rows:
        raise ValueError("No monthly price rows found in Table 1 block")
    return rows


def build_series_payload(rows: list[tuple[str, list[float]]]) -> dict:
    payload = {}
    for column_index, (series_key, label, color) in enumerate(SERIES_CONFIG):
        payload[series_key] = {
            "label": label,
            "unit": "US cents/lb",
            "color": color,
            "points": [
                {
                    "label": row_label,
                    "date": parse_month_label(row_label),
                    "value": values[column_index],
                }
                for row_label, values in rows
            ],
        }
    return payload


def export_trend_data(input_dir: Path, output_path: Path) -> dict:
    report = latest_report(input_dir)
    table_block = extract_table_block(report["full_text"], "Table 1:", "Table 2:")
    rows = parse_monthly_price_rows(table_block)

    payload = {
        "source_report": {
            "report_id": report["report_id"],
            "title": report["title"],
            "published_date": report["published_date"],
            "source_url": report["source_url"],
        },
        "series": build_series_payload(rows),
    }

    write_json(output_path, payload)
    print(f"Wrote trend data to {output_path}")
    return payload


def main() -> int:
    args = parse_args()
    export_trend_data(Path(args.input_dir), Path(args.output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
