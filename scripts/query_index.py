#!/usr/bin/env python3
"""Query the local TF-IDF index and print the top matching chunks."""

from __future__ import annotations

import argparse
import pickle
import sys
from pathlib import Path

from sklearn.metrics.pairwise import cosine_similarity

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_INDEX = Path("data/processed/ico/index/tfidf_index.pkl")


def load_index(index_path: Path) -> dict:
    with index_path.open("rb") as handle:
        return pickle.load(handle)


def search_index(index: dict, query: str, top_k: int) -> list[dict]:
    vectorizer = index["vectorizer"]
    matrix = index["matrix"]
    chunks = index["chunks"]

    query_vector = vectorizer.transform([query])
    scores = cosine_similarity(query_vector, matrix).ravel()
    ranked_indices = scores.argsort()[::-1][:top_k]

    results: list[dict] = []
    for position in ranked_indices:
        chunk = dict(chunks[position])
        chunk["score"] = float(scores[position])
        results.append(chunk)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="Search query")
    parser.add_argument("--index-path", default=str(DEFAULT_INDEX))
    parser.add_argument("--top-k", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    index = load_index(Path(args.index_path))
    results = search_index(index, args.query, args.top_k)

    for rank, result in enumerate(results, start=1):
        print(f"[{rank}] score={result['score']:.4f} report={result['report_id']} page={result['page_number']}")
        print(result["title"])
        print(result["chunk_text"][:600].strip())
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
