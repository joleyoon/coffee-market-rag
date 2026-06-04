"""Shared helpers for sentence-transformers embeddings and FAISS indexes."""

from __future__ import annotations

from functools import lru_cache
from typing import Protocol

import numpy as np


DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


class EmbeddingModel(Protocol):
    def encode(self, sentences: list[str], **kwargs: object) -> object:
        """Return embeddings for a batch of strings."""


@lru_cache(maxsize=4)
def load_embedding_model(model_name: str = DEFAULT_EMBEDDING_MODEL) -> EmbeddingModel:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError(
            "sentence-transformers is required for embedding search. "
            "Install dependencies with `python3 -m pip install -r requirements.txt`."
        ) from exc

    return SentenceTransformer(model_name)


def require_faiss():
    try:
        import faiss
    except ImportError as exc:
        raise RuntimeError(
            "faiss-cpu is required for vector search. "
            "Install dependencies with `python3 -m pip install -r requirements.txt`."
        ) from exc

    return faiss


def encode_texts(model: EmbeddingModel, texts: list[str], batch_size: int = 32) -> np.ndarray:
    if not texts:
        raise ValueError("Cannot encode an empty text batch")

    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        convert_to_numpy=True,
        normalize_embeddings=False,
        show_progress_bar=False,
    )
    array = np.asarray(embeddings, dtype=np.float32)
    if array.ndim != 2:
        raise ValueError(f"Expected a 2D embedding matrix, got shape {array.shape}")

    norms = np.linalg.norm(array, axis=1, keepdims=True)
    np.divide(array, np.maximum(norms, 1e-12), out=array)
    return np.ascontiguousarray(array, dtype=np.float32)


def build_faiss_index(embeddings: np.ndarray):
    if embeddings.ndim != 2 or embeddings.shape[0] == 0:
        raise ValueError(f"Expected a non-empty 2D embedding matrix, got shape {embeddings.shape}")

    faiss = require_faiss()
    index = faiss.IndexFlatIP(embeddings.shape[1])
    index.add(embeddings)
    return index


def serialize_faiss_index(index: object) -> bytes:
    faiss = require_faiss()
    return faiss.serialize_index(index).tobytes()


def deserialize_faiss_index(serialized_index: object):
    faiss = require_faiss()
    if isinstance(serialized_index, bytes):
        buffer = np.frombuffer(serialized_index, dtype=np.uint8).copy()
        return faiss.deserialize_index(buffer)
    return serialized_index
