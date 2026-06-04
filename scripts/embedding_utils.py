"""Shared helpers for sentence-transformers embeddings and FAISS indexes."""

from __future__ import annotations

import hashlib
import re
from functools import lru_cache
from typing import Protocol

import numpy as np


DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
LOCAL_SMOKE_EMBEDDING_MODEL = "local-ci-smoke-embeddings"
_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


class EmbeddingModel(Protocol):
    def encode(self, sentences: list[str], **kwargs: object) -> object:
        """Return embeddings for a batch of strings."""


class LocalSmokeEmbeddingModel:
    """Small deterministic embedding model for CI smoke tests."""

    def __init__(self, dimensions: int = 64) -> None:
        self.dimensions = dimensions

    def encode(self, sentences: list[str], **_: object) -> np.ndarray:
        rows = np.zeros((len(sentences), self.dimensions), dtype=np.float32)
        for row_index, sentence in enumerate(sentences):
            for token in _TOKEN_RE.findall(sentence.lower()):
                digest = hashlib.blake2b(token.encode("utf-8"), digest_size=4).digest()
                bucket = int.from_bytes(digest, byteorder="little") % self.dimensions
                rows[row_index, bucket] += 1.0
        return rows


def embedding_backend_name(model_name: str) -> str:
    if model_name == LOCAL_SMOKE_EMBEDDING_MODEL:
        return "local-ci-smoke+faiss"
    return "sentence-transformers+faiss"


@lru_cache(maxsize=4)
def load_embedding_model(model_name: str = DEFAULT_EMBEDDING_MODEL) -> EmbeddingModel:
    if model_name == LOCAL_SMOKE_EMBEDDING_MODEL:
        return LocalSmokeEmbeddingModel()

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
