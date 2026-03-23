#!/usr/bin/env python3
"""Tiny CLI demo for querying the coffee market index."""

from __future__ import annotations

import argparse
import re
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
    parser.add_argument("--max-sentences", type=int, default=4)
    parser.add_argument(
        "--show-context",
        action="store_true",
        help="Print the retrieved supporting snippets after the answer",
    )
    parser.add_argument("query", nargs="*")
    return parser.parse_args()


def split_sentences(text: str) -> list[str]:
    normalized = re.sub(r"\s*•\s*", ". ", text.strip())
    normalized = normalized.replace("\n", " ")
    sentences = re.split(r"(?<=[.!?])\s+", normalized)
    return [sentence.strip() for sentence in sentences if sentence.strip()]


def normalize_sentence(text: str) -> str:
    return re.sub(r"\W+", "", text).lower()


def query_terms(query: str) -> set[str]:
    return {
        term
        for term in re.findall(r"[a-zA-Z]{3,}", query.lower())
        if term not in {"what", "which", "where", "from", "with", "that", "have", "this"}
    }


def sentence_score(sentence: str, query: str, retrieval_score: float) -> float:
    terms = query_terms(query)
    sentence_terms = set(re.findall(r"[a-zA-Z]{3,}", sentence.lower()))
    overlap = len(terms & sentence_terms)
    return retrieval_score + (overlap * 0.02)


def clean_candidate_sentence(sentence: str) -> str:
    sentence = re.sub(r"Coffee Market Report\s*[–-]\s*[A-Za-z]+\s+\d{4}\s*\d*", "", sentence)
    sentence = re.sub(r"Figure\s+[A-Za-z0-9:.\- ]+", "", sentence)
    sentence = re.sub(r"\s+", " ", sentence)
    return sentence.strip(" -")


def is_usable_sentence(sentence: str) -> bool:
    words = sentence.split()
    if len(words) < 10 or len(words) > 55:
        return False
    if "figure" in sentence.lower() or "table" in sentence.lower():
        return False
    if "60-kg bags" in sentence.lower() or "60 -kg bags" in sentence.lower():
        return False
    letter_count = sum(character.isalpha() for character in sentence)
    digit_count = sum(character.isdigit() for character in sentence)
    if letter_count == 0 or digit_count > letter_count:
        return False
    numeric_tokens = sum(bool(re.fullmatch(r"[\d./%-]+", word)) for word in words)
    if numeric_tokens > 4:
        return False
    return True


def build_answer(results: list[dict], query: str, max_sentences: int) -> tuple[list[str], list[str]]:
    ranked_sentences: list[tuple[float, str, str]] = []

    for result in results:
        source = f"{result['title']}, page {result['page_number']}"
        for sentence in split_sentences(result["chunk_text"]):
            cleaned_sentence = clean_candidate_sentence(sentence)
            if not is_usable_sentence(cleaned_sentence):
                continue
            ranked_sentences.append(
                (sentence_score(cleaned_sentence, query, result["score"]), cleaned_sentence, source)
            )

    ranked_sentences.sort(key=lambda item: item[0], reverse=True)

    selected_sentences: list[str] = []
    selected_sources: list[str] = []
    seen_sentences: set[str] = set()

    for _, sentence, source in ranked_sentences:
        normalized = normalize_sentence(sentence)
        if normalized in seen_sentences:
            continue
        seen_sentences.add(normalized)
        selected_sentences.append(sentence)
        if source not in selected_sources:
            selected_sources.append(source)
        if len(selected_sentences) >= max_sentences:
            break

    return selected_sentences, selected_sources


def main() -> int:
    args = parse_args()
    query = " ".join(args.query).strip()
    if not query:
        query = input("Ask about the ICO coffee market reports: ").strip()

    index = load_index(Path(args.index_path))
    results = search_index(index, query, args.top_k)

    answer_sentences, sources = build_answer(results, query, args.max_sentences)

    print(f"Question: {query}\n")
    print("Answer:")
    if answer_sentences:
        print(" ".join(answer_sentences))
    else:
        print("The current index did not return enough evidence to generate a concise answer.")

    if sources:
        print("\nSources:")
        for source in sources:
            print(f"- {source}")

    if args.show_context:
        print("\nSupporting Chunks:")
        for rank, result in enumerate(results, start=1):
            print(f"[{rank}] {result['title']} (page {result['page_number']}, score={result['score']:.4f})")
            print(result["chunk_text"][:500].strip())
            print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
