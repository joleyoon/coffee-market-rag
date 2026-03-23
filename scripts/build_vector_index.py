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


DEFAULT_INPUT = Path("data/processed/ico/chunks/chunks.jsonl")
DEFAULT_OUTPUT = Path("data/processed/ico/index/tfidf_index.pkl")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--max-features", type=int, default=50000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    chunks = load_jsonl(Path(args.input_path))
    documents = [chunk["chunk_text"] for chunk in chunks]

    vectorizer = TfidfVectorizer(
        stop_words="english",
        ngram_range=(1, 2),
        max_features=args.max_features,
    )
    matrix = vectorizer.fit_transform(documents)

    payload = {
        "vectorizer": vectorizer,
        "matrix": matrix,
        "chunks": chunks,
    }

    output_path = Path(args.output_path)
    ensure_directory(output_path.parent)
    with output_path.open("wb") as handle:
        pickle.dump(payload, handle)

    print(f"Indexed {len(chunks)} chunks to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
