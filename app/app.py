#!/usr/bin/env python3
"""CLI and web app for the Coffee Market Intelligence Assistant."""

from __future__ import annotations

import argparse
import html
import json
import mimetypes
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.query_index import filters_from_args, load_index, search_index
from scripts.report_utils import clean_text, load_json


DEFAULT_INDEX = Path("data/processed/ico/index/faiss_index.pkl")
DEFAULT_TREND_DATA = Path("data/processed/ico/trends/trend-data.json")
STATIC_DIR = ROOT / "app" / "static"
DEFAULT_LLM_MODEL = "gpt-5.5"
DEFAULT_OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_SUGGESTIONS = [
    "Which coffee category had the steepest price decline in February 2026?",
    "What factors pushed coffee prices down in early 2026?",
    "What does the ICO say about Brazil's supply outlook?",
    "Which regions showed weaker export performance recently?",
]


@dataclass(frozen=True)
class LLMConfig:
    mode: str
    model: str
    api_key: str | None
    base_url: str
    timeout: float
    max_output_tokens: int

    @property
    def enabled(self) -> bool:
        return self.mode != "off" and bool(self.api_key)

    @property
    def required(self) -> bool:
        return self.mode == "required"


PROJECT_HIGHLIGHTS = [
    "Built a retrieval-augmented generation (RAG) system with optional LLM generation to automate cited insights from coffee market reports.",
    "Built automated ingestion and retrieval workflows with embeddings and FAISS vector search.",
    "Optimized retrieval workflows with metadata filters, normalized vectors, and ranked evidence selection.",
    "Processed and embedded unstructured PDF reports for scalable semantic search and analysis.",
    "Implemented a CI/CD pipeline with GitHub Actions for testing, deployment, smoke checks, and scheduled refreshes.",
]


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
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Run the chatbot website locally instead of the CLI view",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--country", action="append", default=[])
    parser.add_argument("--coffee-type", action="append", default=[])
    parser.add_argument("--published-after", default=None)
    parser.add_argument("--published-before", default=None)
    parser.add_argument("--dataset-version", default=None)
    parser.add_argument(
        "--llm-mode",
        choices=["auto", "off", "required"],
        default=os.environ.get("RAG_LLM_MODE", "auto"),
        help="Use optional LLM generation after retrieval. auto uses OPENAI_API_KEY when present.",
    )
    parser.add_argument("--llm-model", default=os.environ.get("OPENAI_MODEL", DEFAULT_LLM_MODEL))
    parser.add_argument("--llm-timeout", type=float, default=float(os.environ.get("OPENAI_TIMEOUT", "20")))
    parser.add_argument("--llm-max-output-tokens", type=int, default=int(os.environ.get("OPENAI_MAX_OUTPUT_TOKENS", "500")))
    parser.add_argument("query", nargs="*")
    return parser.parse_args()


def split_sentences(text: str) -> list[str]:
    normalized = clean_text(re.sub(r"\s*•\s*", ". ", text.strip()))
    sentences = re.split(r"(?<=[.!?])\s+", normalized)
    return [sentence.strip() for sentence in sentences if sentence.strip()]


def normalize_sentence(text: str) -> str:
    return re.sub(r"\W+", "", text).lower()


def query_terms(query: str) -> set[str]:
    return {
        term
        for term in re.findall(r"[a-zA-Z]{3,}", query.lower())
        if term not in {"what", "which", "where", "from", "with", "that", "have", "this", "recently"}
    }


def sentence_score(sentence: str, query: str, retrieval_score: float) -> float:
    terms = query_terms(query)
    sentence_terms = set(re.findall(r"[a-zA-Z]{3,}", sentence.lower()))
    overlap = len(terms & sentence_terms)
    return retrieval_score + (overlap * 0.02)


def extract_price_declines(text: str) -> list[tuple[str, float, str]]:
    candidates: list[tuple[str, float, str]] = []
    cleaned = clean_text(text)
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)

    for sentence in sentences:
        paired_match = re.search(
            r"Colombian Milds.? and Other Milds.? prices (?:retracted|decreased|declined) ([\d.]+)% and ([\d.]+)%",
            sentence,
            re.IGNORECASE,
        )
        if paired_match:
            candidates.append(("Colombian Milds", float(paired_match.group(1)), sentence))
            candidates.append(("Other Milds", float(paired_match.group(2)), sentence))

        single_patterns = [
            ("Brazilian Naturals", r"Brazilian Naturals.? prices (?:shrank|decreased|declined|fell) ([\d.]+)%"),
            ("Robustas", r"Robustas (?:declined|decreased|fell|contracted) by ([\d.]+)%"),
            ("Colombian Milds", r"Colombian Milds.? prices (?:retracted|decreased|declined|fell) ([\d.]+)%"),
            ("Other Milds", r"Other Milds.? prices (?:retracted|decreased|declined|fell) ([\d.]+)%"),
        ]
        for label, pattern in single_patterns:
            for match in re.finditer(pattern, sentence, re.IGNORECASE):
                candidates.append((label, float(match.group(1)), sentence))

    return candidates


def clean_candidate_sentence(sentence: str) -> str:
    sentence = re.sub(r"Coffee Market Report\s*[–-]\s*[A-Za-z]+\s+\d{4}\s*\d*", "", sentence)
    sentence = re.sub(r"Figure\s+[A-Za-z0-9:.\- ]+", "", sentence)
    sentence = re.sub(r"^[-:;,.\s]+", "", sentence)
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
    if numeric_tokens > 4 or sentence.endswith(":"):
        return False
    return True


def build_answer(results: list[dict], query: str, max_sentences: int) -> tuple[str | None, list[str], list[str]]:
    query_lower = query.lower()

    if ("steepest" in query_lower or "worst" in query_lower) and any(
        phrase in query_lower for phrase in {"price decline", "price performance", "performed worst", "performing worst"}
    ):
        decline_candidates: list[tuple[str, float, str, str]] = []
        for result in results:
            source = f"{result['title']}, page {result['page_number']}"
            for label, percentage, sentence in extract_price_declines(result["chunk_text"]):
                decline_candidates.append((label, percentage, sentence, source))

        if decline_candidates:
            label, percentage, sentence, source = max(decline_candidates, key=lambda item: item[1])
            direct_answer = f"{label} had the steepest price decline at {percentage:.1f}%."
            return direct_answer, [sentence], [source]

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

    direct_answer = selected_sentences[0] if selected_sentences else None
    explanation = selected_sentences[1:] if len(selected_sentences) > 1 else []
    return direct_answer, explanation, selected_sources


def sources_from_results(results: list[dict], selected_sources: list[str]) -> list[dict]:
    source_map = {
        f"{result['title']}, page {result['page_number']}": {
            "title": result["title"],
            "page_number": result["page_number"],
            "report_id": result["report_id"],
            "published_date": result.get("published_date"),
            "source_url": result["source_url"],
            "country_tags": result.get("country_tags", []),
            "coffee_type_tags": result.get("coffee_type_tags", []),
            "dataset_version": result.get("dataset_version"),
        }
        for result in results
    }
    for index, result in enumerate(results, start=1):
        source_map[f"S{index}"] = {
            "title": result["title"],
            "page_number": result["page_number"],
            "report_id": result["report_id"],
            "published_date": result.get("published_date"),
            "source_url": result["source_url"],
            "country_tags": result.get("country_tags", []),
            "coffee_type_tags": result.get("coffee_type_tags", []),
            "dataset_version": result.get("dataset_version"),
        }
    return [source_map[source] for source in selected_sources if source in source_map]


def llm_config_from_args(args: argparse.Namespace) -> LLMConfig:
    return LLMConfig(
        mode=args.llm_mode,
        model=args.llm_model,
        api_key=os.environ.get("OPENAI_API_KEY"),
        base_url=os.environ.get("OPENAI_RESPONSES_URL", DEFAULT_OPENAI_RESPONSES_URL),
        timeout=args.llm_timeout,
        max_output_tokens=args.llm_max_output_tokens,
    )


def context_for_llm(results: list[dict], max_chars_per_chunk: int = 1400) -> str:
    context_blocks: list[str] = []
    for index, result in enumerate(results, start=1):
        source_id = f"S{index}"
        chunk_text = clean_text(result["chunk_text"])[:max_chars_per_chunk]
        tags = ", ".join(result.get("country_tags", []) + result.get("coffee_type_tags", [])) or "n/a"
        context_blocks.append(
            "\n".join(
                [
                    f"[{source_id}] {result['title']}, page {result['page_number']}",
                    f"published_date: {result.get('published_date') or 'n/a'}",
                    f"metadata_tags: {tags}",
                    f"text: {chunk_text}",
                ]
            )
        )
    return "\n\n".join(context_blocks)


def llm_generation_prompt(query: str, results: list[dict]) -> str:
    return f"""Question:
{query}

Retrieved context:
{context_for_llm(results)}

Return JSON only with this shape:
{{
  "answer": "one concise direct answer grounded only in the retrieved context",
  "why": ["one or two short supporting points"],
  "sources": ["S1", "S2"]
}}

Use only the retrieved context. If the context does not contain enough evidence, set answer to null, why to [], and sources to []."""


def extract_response_text(response_payload: dict) -> str:
    if isinstance(response_payload.get("output_text"), str):
        return response_payload["output_text"]

    text_parts: list[str] = []
    for item in response_payload.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                text_parts.append(content["text"])
    return "\n".join(text_parts).strip()


def call_openai_responses_api(prompt: str, config: LLMConfig) -> dict:
    if not config.api_key:
        raise RuntimeError("OPENAI_API_KEY is required for LLM generation")

    request_payload = {
        "model": config.model,
        "instructions": (
            "You generate grounded coffee market answers from retrieved ICO report chunks. "
            "Use only supplied context, keep answers concise, and cite source IDs exactly."
        ),
        "input": prompt,
        "max_output_tokens": config.max_output_tokens,
        "text": {"format": {"type": "text"}, "verbosity": "low"},
        "reasoning": {"effort": "low"},
        "store": False,
    }
    body = json.dumps(request_payload).encode("utf-8")
    request = urllib.request.Request(
        config.base_url,
        data=body,
        headers={
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=config.timeout) as response:  # noqa: S310
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI response failed with HTTP {exc.code}: {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OpenAI response failed: {exc.reason}") from exc


def parse_llm_answer(raw_text: str) -> tuple[str | None, list[str], list[str]]:
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    payload = json.loads(cleaned)
    answer = payload.get("answer")
    if answer is not None:
        answer = str(answer).strip() or None
    why = [str(item).strip() for item in payload.get("why", []) if str(item).strip()]
    sources = [str(item).strip() for item in payload.get("sources", []) if str(item).strip()]
    return answer, why, sources


def build_llm_answer(results: list[dict], query: str, config: LLMConfig) -> tuple[str | None, list[str], list[str]]:
    response_payload = call_openai_responses_api(llm_generation_prompt(query, results), config)
    raw_text = extract_response_text(response_payload)
    if not raw_text:
        raise RuntimeError("OpenAI response did not include output text")
    return parse_llm_answer(raw_text)


def infer_trend_chart(trend_data: dict | None, query: str, answer: str | None) -> dict | None:
    if not trend_data:
        return None

    series = trend_data.get("series", {})
    text = f"{query} {answer or ''}".lower()

    if any(phrase in text for phrase in {"steepest price decline", "which coffee category", "performed worst", "performing worst", "compare"}):
        keys = ["colombian_milds", "other_milds", "brazilian_naturals", "robustas"]
        return {
            "title": "ICO group price trend",
            "subtitle": "Monthly average prices from the latest ICO report table",
            "unit": "US cents/lb",
            "series": [series[key] for key in keys if key in series],
        }

    series_aliases = [
        ("robustas", ("robusta", "robustas")),
        ("colombian_milds", ("colombian milds",)),
        ("other_milds", ("other milds",)),
        ("brazilian_naturals", ("brazilian naturals", "brazilian natural")),
        ("new_york", ("new york",)),
        ("london", ("london",)),
    ]

    for key, aliases in series_aliases:
        if any(alias in text for alias in aliases) and key in series:
            return {
                "title": series[key]["label"] + " trend",
                "subtitle": "Monthly average prices from the latest ICO report table",
                "unit": series[key]["unit"],
                "series": [series[key]],
            }

    if any(keyword in text for keyword in {"price", "prices", "i-cip", "composite", "market performance", "decline"}):
        key = "ico_composite"
        if key in series:
            return {
                "title": "ICO Composite Indicator Price trend",
                "subtitle": "Monthly average prices from the latest ICO report table",
                "unit": series[key]["unit"],
                "series": [series[key]],
            }

    return None


def answer_query(
    index: dict,
    query: str,
    top_k: int,
    max_sentences: int,
    trend_data: dict | None = None,
    filters: dict | None = None,
    llm_config: LLMConfig | None = None,
) -> dict:
    results = search_index(index, query, top_k, filters=filters)
    direct_answer, explanation, selected_sources = build_answer(results, query, max_sentences)
    answer_mode = "extractive"
    llm_error = None

    if llm_config and results:
        if llm_config.required and not llm_config.enabled:
            raise RuntimeError("LLM generation is required, but OPENAI_API_KEY is not set")
        if llm_config.enabled:
            try:
                llm_answer, llm_explanation, llm_sources = build_llm_answer(results, query, llm_config)
                if llm_answer:
                    direct_answer = llm_answer
                    explanation = llm_explanation
                    selected_sources = llm_sources
                    answer_mode = "llm"
            except (RuntimeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                if llm_config.required:
                    raise RuntimeError(f"LLM generation failed: {exc}") from exc
                llm_error = str(exc)

    index_metadata = index.get("metadata", {})

    return {
        "query": query,
        "filters": filters or {},
        "dataset_version": index_metadata.get("dataset_version"),
        "answer_mode": answer_mode,
        "llm_model": llm_config.model if llm_config and answer_mode == "llm" else None,
        "llm_error": llm_error,
        "answer": direct_answer,
        "why": explanation,
        "sources": sources_from_results(results, selected_sources),
        "trend_chart": infer_trend_chart(trend_data, query, direct_answer),
        "results": [
            {
                "title": result["title"],
                "page_number": result["page_number"],
                "score": round(result["score"], 4),
                "published_date": result.get("published_date"),
                "country_tags": result.get("country_tags", []),
                "coffee_type_tags": result.get("coffee_type_tags", []),
                "chunk_text": result["chunk_text"][:500].strip(),
            }
            for result in results
        ],
    }


def print_cli_response(payload: dict, show_context: bool) -> None:
    print(f"Question: {payload['query']}\n")
    print(f"Mode: {payload.get('answer_mode', 'extractive')}")
    print("Answer:")
    print(payload["answer"] or "The current index did not return enough evidence to generate a concise answer.")

    if payload["why"]:
        print("\nWhy:")
        print(" ".join(payload["why"]))

    if payload["sources"]:
        print("\nSources:")
        for source in payload["sources"]:
            print(f"- {source['title']}, page {source['page_number']}")

    if show_context and payload["results"]:
        print("\nSupporting Chunks:")
        for rank, result in enumerate(payload["results"], start=1):
            print(f"[{rank}] {result['title']} (page {result['page_number']}, score={result['score']:.4f})")
            print(result["chunk_text"])
            print()


def app_metrics(index: dict) -> dict:
    chunks = index["chunks"]
    report_ids = {chunk["report_id"] for chunk in chunks}
    dates = sorted(chunk["published_date"] for chunk in chunks if chunk.get("published_date"))
    index_metadata = index.get("metadata", {})
    return {
        "report_count": len(report_ids),
        "chunk_count": len(chunks),
        "start_period": index_metadata.get("start_period") or (dates[0][:7] if dates else "n/a"),
        "end_period": index_metadata.get("end_period") or (dates[-1][:7] if dates else "n/a"),
        "dataset_version": index_metadata.get("dataset_version") or "legacy",
        "embedding_backend": index_metadata.get("embedding_backend") or "sentence-transformers+faiss",
        "embedding_model": index_metadata.get("embedding_model") or "sentence-transformers",
        "vector_index": index_metadata.get("faiss_index_type") or "FAISS",
    }


def build_homepage(metrics: dict, llm_config: LLMConfig | None = None) -> bytes:
    llm_status = "LLM enabled" if llm_config and llm_config.enabled else "LLM optional"
    config = {
        "mode": "live",
        "suggestions": DEFAULT_SUGGESTIONS,
        "reportCount": metrics["report_count"],
        "chunkCount": metrics["chunk_count"],
        "datasetVersion": metrics["dataset_version"],
        "embeddingBackend": metrics["embedding_backend"],
        "embeddingModel": metrics["embedding_model"],
        "vectorIndex": metrics["vector_index"],
        "llmMode": llm_config.mode if llm_config else "auto",
        "llmModel": llm_config.model if llm_config else DEFAULT_LLM_MODEL,
        "llmEnabled": bool(llm_config and llm_config.enabled),
        "systemHighlights": PROJECT_HIGHLIGHTS,
        "localRunCommand": "python3 app/app.py --serve",
        "trendDataUrl": "/static/trend-data.json",
    }

    html_page = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Coffee Market Intelligence Assistant</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Fraunces:wght@500;600;700&family=Source+Sans+3:wght@400;500;600;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="/static/style.css" />
</head>
<body>
  <div class="page-shell">
    <aside class="hero-panel">
      <div class="hero-mark">CM</div>
      <p class="eyebrow">ICO REPORTS / RAG SYSTEM</p>
      <h1>Coffee Market Intelligence Assistant</h1>
      <p class="hero-copy">
        Retrieval-augmented analytics for ICO Coffee Market Reports: ingest PDFs, embed report chunks, search with FAISS, and optionally generate cited market insights with an LLM.
      </p>

      <div class="hero-stats">
        <article class="stat-card">
          <span class="stat-label">Coverage</span>
          <strong class="stat-value">{metrics['report_count']} reports</strong>
          <p>{metrics['start_period']} to {metrics['end_period']}</p>
        </article>
        <article class="stat-card">
          <span class="stat-label">Vector Search</span>
          <strong class="stat-value">{metrics['chunk_count']} chunks</strong>
          <p>{html.escape(metrics['vector_index'])} over sentence-transformers embeddings</p>
        </article>
        <article class="stat-card">
          <span class="stat-label">Automation</span>
          <strong class="stat-value">CI/CD</strong>
          <p>GitHub Actions tests, smoke checks, deployment, and scheduled refreshes</p>
        </article>
        <article class="stat-card">
          <span class="stat-label">Generation</span>
          <strong class="stat-value">{html.escape(llm_status)}</strong>
          <p>OpenAI Responses API when OPENAI_API_KEY is configured</p>
        </article>
      </div>

      <section class="system-panel">
        <h2>System Highlights</h2>
        <ul class="system-list">
          {"".join(f'<li>{html.escape(item)}</li>' for item in PROJECT_HIGHLIGHTS)}
        </ul>
      </section>

      <section class="suggestion-panel">
        <h2>Prompt Ideas</h2>
        <div class="suggestion-list">
          {"".join(f'<button class="suggestion-chip" data-suggestion="{html.escape(prompt)}">{html.escape(prompt)}</button>' for prompt in DEFAULT_SUGGESTIONS)}
        </div>
      </section>
    </aside>

    <section class="chat-panel">
      <header class="chat-header">
        <div>
          <p class="eyebrow">LIVE RAG WORKFLOW</p>
          <h2>Ask the embedded market index</h2>
        </div>
        <div class="status-pill">
          <span class="status-dot"></span>
          {html.escape(metrics['embedding_backend'])} / {html.escape(llm_status)}
        </div>
      </header>

      <div id="messages" class="messages"></div>

      <form id="chat-form" class="composer">
        <label class="composer-label" for="query-input">Ask about prices, exports, supply, weather, or country performance.</label>
        <div class="composer-row">
          <textarea id="query-input" name="query" rows="2" placeholder="What factors pushed coffee prices down in early 2026?"></textarea>
          <button type="submit" id="send-button">Ask</button>
        </div>
      </form>
    </section>
  </div>

  <script>
    window.APP_CONFIG = {json.dumps(config)};
  </script>
  <script src="/static/chat.js"></script>
</body>
</html>
"""
    return html_page.encode("utf-8")


def serve_file(handler: BaseHTTPRequestHandler, file_path: Path) -> None:
    if not file_path.exists() or not file_path.is_file():
        handler.send_error(404)
        return

    content_type, _ = mimetypes.guess_type(str(file_path))
    handler.send_response(200)
    handler.send_header("Content-Type", content_type or "application/octet-stream")
    handler.end_headers()
    handler.wfile.write(file_path.read_bytes())


def make_handler(
    index: dict,
    metrics: dict,
    top_k: int,
    max_sentences: int,
    trend_data: dict | None,
    llm_config: LLMConfig | None,
):
    class CoffeeHandler(BaseHTTPRequestHandler):
        def _send_json(self, payload: dict, status: int = 200) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            return json.loads(raw.decode("utf-8"))

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/":
                body = build_homepage(metrics, llm_config=llm_config)
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if parsed.path.startswith("/static/"):
                relative = parsed.path.removeprefix("/static/")
                serve_file(self, STATIC_DIR / relative)
                return

            if parsed.path == "/api/health":
                self._send_json(
                    {
                        "ok": True,
                        "report_count": metrics["report_count"],
                        "chunk_count": metrics["chunk_count"],
                        "dataset_version": metrics["dataset_version"],
                        "coverage": {
                            "start_period": metrics["start_period"],
                            "end_period": metrics["end_period"],
                        },
                    }
                )
                return

            self.send_error(404)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/api/chat":
                self.send_error(404)
                return

            try:
                payload = self._read_json()
            except json.JSONDecodeError:
                self._send_json({"error": "Invalid JSON payload"}, status=400)
                return

            query = (payload.get("query") or "").strip()
            if not query:
                self._send_json({"error": "Query is required"}, status=400)
                return

            filters = payload.get("filters") if isinstance(payload.get("filters"), dict) else None
            try:
                response = answer_query(
                    index,
                    query,
                    top_k=top_k,
                    max_sentences=max_sentences,
                    trend_data=trend_data,
                    filters=filters,
                    llm_config=llm_config,
                )
            except RuntimeError as exc:
                self._send_json({"error": str(exc)}, status=502)
                return
            self._send_json(response)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    return CoffeeHandler


def run_server(
    index_path: Path,
    host: str,
    port: int,
    top_k: int,
    max_sentences: int,
    llm_config: LLMConfig | None = None,
) -> None:
    index = load_index(index_path)
    metrics = app_metrics(index)
    trend_data = load_json(DEFAULT_TREND_DATA) if DEFAULT_TREND_DATA.exists() else None
    handler = make_handler(index, metrics, top_k, max_sentences, trend_data, llm_config)
    server = ThreadingHTTPServer((host, port), handler)
    print(f"Serving Coffee Market Intelligence Assistant at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
    finally:
        server.server_close()


def main() -> int:
    args = parse_args()
    llm_config = llm_config_from_args(args)

    if args.serve:
        run_server(Path(args.index_path), args.host, args.port, args.top_k, args.max_sentences, llm_config=llm_config)
        return 0

    query = " ".join(args.query).strip()
    if not query:
        query = input("Ask about the ICO coffee market reports: ").strip()

    index = load_index(Path(args.index_path))
    trend_data = load_json(DEFAULT_TREND_DATA) if DEFAULT_TREND_DATA.exists() else None
    try:
        payload = answer_query(
            index,
            query,
            top_k=args.top_k,
            max_sentences=args.max_sentences,
            trend_data=trend_data,
            filters=filters_from_args(args),
            llm_config=llm_config,
        )
    except RuntimeError as exc:
        print(f"Request failed: {exc}", file=sys.stderr)
        return 1
    print_cli_response(payload, args.show_context)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
