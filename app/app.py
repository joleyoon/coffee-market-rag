#!/usr/bin/env python3
"""CLI and web app for the Coffee Market Intelligence Assistant."""

from __future__ import annotations

import argparse
import html
import json
import mimetypes
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.query_index import load_index, search_index
from scripts.report_utils import clean_text, load_json


DEFAULT_INDEX = Path("data/processed/ico/index/tfidf_index.pkl")
DEFAULT_TREND_DATA = Path("data/processed/ico/trends/trend-data.json")
STATIC_DIR = ROOT / "app" / "static"
DEFAULT_SUGGESTIONS = [
    "Which coffee category had the steepest price decline in February 2026?",
    "What factors pushed coffee prices down in early 2026?",
    "What does the ICO say about Brazil's supply outlook?",
    "Which regions showed weaker export performance recently?",
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
        }
        for result in results
    }
    return [source_map[source] for source in selected_sources if source in source_map]


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


def answer_query(index: dict, query: str, top_k: int, max_sentences: int, trend_data: dict | None = None) -> dict:
    results = search_index(index, query, top_k)
    direct_answer, explanation, selected_sources = build_answer(results, query, max_sentences)

    return {
        "query": query,
        "answer": direct_answer,
        "why": explanation,
        "sources": sources_from_results(results, selected_sources),
        "trend_chart": infer_trend_chart(trend_data, query, direct_answer),
        "results": [
            {
                "title": result["title"],
                "page_number": result["page_number"],
                "score": round(result["score"], 4),
                "chunk_text": result["chunk_text"][:500].strip(),
            }
            for result in results
        ],
    }


def print_cli_response(payload: dict, show_context: bool) -> None:
    print(f"Question: {payload['query']}\n")
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
    return {
        "report_count": len(report_ids),
        "chunk_count": len(chunks),
        "start_period": dates[0][:7] if dates else "n/a",
        "end_period": dates[-1][:7] if dates else "n/a",
    }


def build_homepage(metrics: dict) -> bytes:
    config = {
        "mode": "live",
        "suggestions": DEFAULT_SUGGESTIONS,
        "reportCount": metrics["report_count"],
        "chunkCount": metrics["chunk_count"],
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
      <p class="eyebrow">ICO REPORTS / RAG PROTOTYPE</p>
      <h1>Coffee Market Intelligence Assistant</h1>
      <p class="hero-copy">
        Ask grounded questions across ICO Coffee Market Reports and get a direct answer, a short explanation, and cited report pages.
      </p>

      <div class="hero-stats">
        <article class="stat-card">
          <span class="stat-label">Coverage</span>
          <strong class="stat-value">{metrics['report_count']} reports</strong>
          <p>{metrics['start_period']} to {metrics['end_period']}</p>
        </article>
        <article class="stat-card">
          <span class="stat-label">Retrieval Layer</span>
          <strong class="stat-value">{metrics['chunk_count']} chunks</strong>
          <p>ICO coffee market report passages indexed locally</p>
        </article>
      </div>

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
          <p class="eyebrow">LIVE CHAT</p>
          <h2>Grounded in ICO market reports</h2>
        </div>
        <div class="status-pill">
          <span class="status-dot"></span>
          Retrieval + answer synthesis
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


def make_handler(index: dict, metrics: dict, top_k: int, max_sentences: int, trend_data: dict | None):
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
                body = build_homepage(metrics)
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
                self._send_json({"ok": True, "report_count": metrics["report_count"]})
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

            response = answer_query(index, query, top_k=top_k, max_sentences=max_sentences, trend_data=trend_data)
            self._send_json(response)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    return CoffeeHandler


def run_server(index_path: Path, host: str, port: int, top_k: int, max_sentences: int) -> None:
    index = load_index(index_path)
    metrics = app_metrics(index)
    trend_data = load_json(DEFAULT_TREND_DATA) if DEFAULT_TREND_DATA.exists() else None
    handler = make_handler(index, metrics, top_k, max_sentences, trend_data)
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

    if args.serve:
        run_server(Path(args.index_path), args.host, args.port, args.top_k, args.max_sentences)
        return 0

    query = " ".join(args.query).strip()
    if not query:
        query = input("Ask about the ICO coffee market reports: ").strip()

    index = load_index(Path(args.index_path))
    trend_data = load_json(DEFAULT_TREND_DATA) if DEFAULT_TREND_DATA.exists() else None
    payload = answer_query(index, query, top_k=args.top_k, max_sentences=args.max_sentences, trend_data=trend_data)
    print_cli_response(payload, args.show_context)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
