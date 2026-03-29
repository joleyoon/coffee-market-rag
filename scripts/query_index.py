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


def listify(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def load_index(index_path: Path) -> dict:
    with index_path.open("rb") as handle:
        return pickle.load(handle)


def chunk_matches_filters(chunk: dict, filters: dict | None) -> bool:
    if not filters:
        return True

    countries = {value.lower() for value in chunk.get("country_tags", [])}
    coffee_types = {value.lower() for value in chunk.get("coffee_type_tags", [])}
    published_date = chunk.get("published_date")
    dataset_version = chunk.get("dataset_version")

    requested_countries = {value.lower() for value in listify(filters.get("countries"))}
    if requested_countries and countries.isdisjoint(requested_countries):
        return False

    requested_coffee_types = {value.lower() for value in listify(filters.get("coffee_types"))}
    if requested_coffee_types and coffee_types.isdisjoint(requested_coffee_types):
        return False

    published_after = filters.get("published_after")
    if published_after and (not published_date or published_date < published_after):
        return False

    published_before = filters.get("published_before")
    if published_before and (not published_date or published_date > published_before):
        return False

    requested_version = filters.get("dataset_version")
    if requested_version and dataset_version != requested_version:
        return False

    return True


def search_index(index: dict, query: str, top_k: int, filters: dict | None = None) -> list[dict]:
    vectorizer = index["vectorizer"]
    matrix = index["matrix"]
    chunks = index["chunks"]

    query_vector = vectorizer.transform([query])
    allowed_positions = [position for position, chunk in enumerate(chunks) if chunk_matches_filters(chunk, filters)]
    if not allowed_positions:
        return []

    filtered_matrix = matrix[allowed_positions]
    scores = cosine_similarity(query_vector, filtered_matrix).ravel()
    ranked_indices = scores.argsort()[::-1][:top_k]

    results: list[dict] = []
    for position in ranked_indices:
        chunk = dict(chunks[allowed_positions[position]])
        chunk["score"] = float(scores[position])
        results.append(chunk)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="Search query")
    parser.add_argument("--index-path", default=str(DEFAULT_INDEX))
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--country", action="append", default=[])
    parser.add_argument("--coffee-type", action="append", default=[])
    parser.add_argument("--published-after", default=None)
    parser.add_argument("--published-before", default=None)
    parser.add_argument("--dataset-version", default=None)
    return parser.parse_args()


def filters_from_args(args: argparse.Namespace) -> dict | None:
    filters = {
        "countries": args.country,
        "coffee_types": args.coffee_type,
        "published_after": args.published_after,
        "published_before": args.published_before,
        "dataset_version": args.dataset_version,
    }
    if not any(filters.values()):
        return None
    return filters


def main() -> int:
    args = parse_args()
    index = load_index(Path(args.index_path))
    results = search_index(index, args.query, args.top_k, filters=filters_from_args(args))

    for rank, result in enumerate(results, start=1):
        print(f"[{rank}] score={result['score']:.4f} report={result['report_id']} page={result['page_number']}")
        print(result["title"])
        print(result["chunk_text"][:600].strip())
        if result.get("country_tags") or result.get("coffee_type_tags"):
            print(
                f"metadata country={','.join(result.get('country_tags', [])) or 'n/a'} "
                f"coffee_type={','.join(result.get('coffee_type_tags', [])) or 'n/a'} "
                f"date={result.get('published_date') or 'n/a'}"
            )
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
