#!/usr/bin/env python3
"""Build a local sentence-transformers + FAISS retrieval index from report chunks."""

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
    build_faiss_index,
    embedding_backend_name,
    encode_texts,
    load_embedding_model,
    serialize_faiss_index,
)
from scripts.report_utils import ensure_directory, load_jsonl
from scripts.pipeline_utils import iso_timestamp


DEFAULT_INPUT = Path("data/processed/ico/chunks/chunks.jsonl")
DEFAULT_OUTPUT = Path("data/processed/ico/index/faiss_index.pkl")


def embedding_text_for_chunk(chunk: dict) -> str:
    metadata_parts = [
        chunk.get("title"),
        chunk.get("published_date"),
        chunk.get("report_month"),
        chunk.get("report_year"),
        " ".join(chunk.get("country_tags", [])),
        " ".join(chunk.get("coffee_type_tags", [])),
        chunk.get("report_period"),
    ]
    return "\n".join(part for part in [*metadata_parts, chunk["chunk_text"]] if part)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-features", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--dataset-version", default=None)
    parser.add_argument("--built-at", default=None)
    return parser.parse_args()


def build_index(
    chunks: list[dict],
    output_path: Path,
    max_features: int | None = None,
    embedding_model: EmbeddingModel | None = None,
    embedding_model_name: str = DEFAULT_EMBEDDING_MODEL,
    batch_size: int = 32,
    dataset_version: str | None = None,
    built_at: str | None = None,
) -> dict:
    if not chunks:
        raise ValueError("Cannot build an index with zero chunks")
    _ = max_features

    documents = [embedding_text_for_chunk(chunk) for chunk in chunks]
    model = embedding_model or load_embedding_model(embedding_model_name)
    embeddings = encode_texts(model, documents, batch_size=batch_size)
    faiss_index = build_faiss_index(embeddings)
    report_ids = {chunk["report_id"] for chunk in chunks}
    dates = sorted(chunk["published_date"] for chunk in chunks if chunk.get("published_date"))

    payload = {
        "faiss_index": serialize_faiss_index(faiss_index),
        "chunks": chunks,
        "metadata": {
            "dataset_version": dataset_version,
            "embedding_backend": embedding_backend_name(embedding_model_name),
            "embedding_model": embedding_model_name,
            "distance_metric": "cosine",
            "faiss_index_type": "IndexFlatIP",
            "embedding_text": "metadata+chunk_text",
            "embedding_dimension": int(embeddings.shape[1]),
            "index_size": int(faiss_index.ntotal),
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
        embedding_model_name=args.embedding_model,
        batch_size=args.batch_size,
        dataset_version=args.dataset_version,
        built_at=args.built_at,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
