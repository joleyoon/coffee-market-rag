#!/usr/bin/env python3
"""Tiny CLI demo for querying the coffee market index."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.query_index import load_index, search_index


DEFAULT_INDEX = Path("data/processed/ico/index/tfidf_index.pkl")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-path", default=str(DEFAULT_INDEX))
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("query", nargs="*")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    query = " ".join(args.query).strip()
    if not query:
        query = input("Ask about the ICO coffee market reports: ").strip()

    index = load_index(Path(args.index_path))
    results = search_index(index, query, args.top_k)

    print(f"Top {len(results)} results for: {query}\n")
    for rank, result in enumerate(results, start=1):
        print(f"{rank}. {result['title']} (page {result['page_number']}, score={result['score']:.4f})")
        print(result["chunk_text"][:500].strip())
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
