#!/usr/bin/env python3
"""Build a local TF-IDF retrieval index from report chunks."""

from __future__ import annotations

import argparse
import pickle
import sys
from pathlib import Path

from sklearn.feature_extraction.text import TfidfVectorizer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_utils import ensure_directory, load_jsonl
from scripts.pipeline_utils import iso_timestamp


DEFAULT_INPUT = Path("data/processed/ico/chunks/chunks.jsonl")
DEFAULT_OUTPUT = Path("data/processed/ico/index/tfidf_index.pkl")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--max-features", type=int, default=50000)
    parser.add_argument("--dataset-version", default=None)
    parser.add_argument("--built-at", default=None)
    return parser.parse_args()


def build_index(
    chunks: list[dict],
    output_path: Path,
    max_features: int = 50000,
    dataset_version: str | None = None,
    built_at: str | None = None,
) -> dict:
    if not chunks:
        raise ValueError("Cannot build an index with zero chunks")

    documents = [chunk["chunk_text"] for chunk in chunks]

    vectorizer = TfidfVectorizer(
        stop_words="english",
        ngram_range=(1, 2),
        max_features=max_features,
    )
    matrix = vectorizer.fit_transform(documents)
    report_ids = {chunk["report_id"] for chunk in chunks}
    dates = sorted(chunk["published_date"] for chunk in chunks if chunk.get("published_date"))

    payload = {
        "vectorizer": vectorizer,
        "matrix": matrix,
        "chunks": chunks,
        "metadata": {
            "dataset_version": dataset_version,
            "embedding_backend": "tfidf",
            "built_at": built_at or iso_timestamp(),
            "chunk_count": len(chunks),
            "report_count": len(report_ids),
            "start_period": dates[0][:7] if dates else None,
            "end_period": dates[-1][:7] if dates else None,
        },
    }

    ensure_directory(output_path.parent)
    with output_path.open("wb") as handle:
        pickle.dump(payload, handle)

    print(f"Indexed {len(chunks)} chunks to {output_path}")
    return payload


def main() -> int:
    args = parse_args()
    chunks = load_jsonl(Path(args.input_path))
    build_index(
        chunks,
        output_path=Path(args.output_path),
        max_features=args.max_features,
        dataset_version=args.dataset_version,
        built_at=args.built_at,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
