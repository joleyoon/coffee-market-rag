#!/usr/bin/env python3
"""Query the local sentence-transformers + FAISS index and print matching chunks."""

from __future__ import annotations

import argparse
import pickle
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.embedding_utils import (
    DEFAULT_EMBEDDING_MODEL,
    EmbeddingModel,
    deserialize_faiss_index,
    encode_texts,
    load_embedding_model,
)


DEFAULT_INDEX = Path("data/processed/ico/index/faiss_index.pkl")


def listify(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def load_index(index_path: Path) -> dict:
    with index_path.open("rb") as handle:
        return pickle.load(handle)


def index_model_name(index: dict) -> str:
    return index.get("metadata", {}).get("embedding_model") or DEFAULT_EMBEDDING_MODEL


def materialized_faiss_index(index: dict):
    if "_faiss_index" not in index:
        index["_faiss_index"] = deserialize_faiss_index(index["faiss_index"])
    return index["_faiss_index"]


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


def search_index(
    index: dict,
    query: str,
    top_k: int,
    filters: dict | None = None,
    embedding_model: EmbeddingModel | None = None,
) -> list[dict]:
    if top_k <= 0:
        return []

    faiss_index = materialized_faiss_index(index)
    chunks = index["chunks"]
    model = embedding_model or load_embedding_model(index_model_name(index))

    allowed_positions = [position for position, chunk in enumerate(chunks) if chunk_matches_filters(chunk, filters)]
    if not allowed_positions:
        return []

    query_embedding = encode_texts(model, [query], batch_size=1)
    search_limit = len(chunks) if filters else top_k
    scores, labels = faiss_index.search(query_embedding, min(search_limit, faiss_index.ntotal))
    allowed_position_set = set(allowed_positions) if filters else None

    results: list[dict] = []
    for score, position in zip(scores[0], labels[0]):
        if position < 0:
            continue
        if allowed_position_set is not None and int(position) not in allowed_position_set:
            continue
        chunk = dict(chunks[int(position)])
        chunk["score"] = float(score)
        results.append(chunk)
        if len(results) >= top_k:
            break
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
