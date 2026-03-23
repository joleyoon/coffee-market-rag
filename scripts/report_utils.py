"""Shared helpers for the ICO report processing pipeline."""

from __future__ import annotations

import json
import re
from calendar import month_name
from pathlib import Path
from typing import Iterable


MONTH_TO_NUMBER = {
    month.lower(): index
    for index, month in enumerate(month_name)
    if month
}

COMMON_BROKEN_WORDS = {
    "driver s": "drivers",
    "outlook s": "outlooks",
    "continue d": "continued",
    "continuedthe": "continued the",
    "stabili zation": "stabilization",
    "de creased": "decreased",
    "exp anded": "expanded",
    "aff ected": "affected",
    "affect ing": "affecting",
    "ﬁ": "fi",
    "ﬂ": "fl",
    "ha s": "has",
    "b oth": "both",
    "a n": "an",
    "markeda": "marked a",
    "I -CIP": "I-CIP",
}


def clean_text(value: str) -> str:
    value = value.replace("\x00", " ")
    value = value.replace("\u00ad", "")
    value = value.replace("–", "-")
    value = value.replace("—", "-")
    value = value.replace("\n", " ")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"(\w)\s+-\s+(\w)", r"\1-\2", value)
    value = re.sub(r"(\w)-\s+(\w)", r"\1\2", value)
    value = re.sub(r"(\w)\s+([,.;:%])", r"\1\2", value)
    value = re.sub(r"\b([A-Za-z])\s+([A-Za-z]{2,})\b", r"\1\2", value)
    value = re.sub(r"\b([A-Za-z]{2,})\s+([A-Za-z])\b", r"\1\2", value)
    for broken, fixed in COMMON_BROKEN_WORDS.items():
        value = value.replace(broken, fixed)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def report_id_from_filename(filename: str) -> str:
    return Path(filename).stem


def parse_published_date(title: str, filename: str) -> str | None:
    title_match = re.search(r"([A-Za-z]+)\s+(\d{4})", title)
    if title_match:
        month = MONTH_TO_NUMBER.get(title_match.group(1).lower())
        year = int(title_match.group(2))
        if month:
            return f"{year:04d}-{month:02d}-01"

    file_match = re.search(r"cmr-(\d{2})(\d{2})-e\.pdf$", filename)
    if file_match:
        month = int(file_match.group(1))
        year = 2000 + int(file_match.group(2))
        return f"{year:04d}-{month:02d}-01"

    return None


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict | list) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            records.append(json.loads(line))
    return records


def write_jsonl(path: Path, records: Iterable[dict]) -> None:
    ensure_directory(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def chunk_words(text: str, chunk_size: int, overlap: int) -> list[str]:
    words = text.split()
    if not words:
        return []

    if overlap >= chunk_size:
        raise ValueError("overlap must be smaller than chunk_size")

    chunks: list[str] = []
    step = chunk_size - overlap
    for start in range(0, len(words), step):
        chunk = words[start : start + chunk_size]
        if not chunk:
            continue
        chunks.append(" ".join(chunk))
        if start + chunk_size >= len(words):
            break
    return chunks
