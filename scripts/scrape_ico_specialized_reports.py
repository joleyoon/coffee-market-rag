#!/usr/bin/env python3
"""Scrape PDF report links from the ICO specialized reports page.

This script fetches https://ico.org/specialized-reports/, extracts PDF links,
stores a JSON manifest, and can optionally download the files.
"""

from __future__ import annotations

import argparse
import json
import ssl
import re
import sys
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen


DEFAULT_URL = "https://ico.org/specialized-reports/"
DEFAULT_OUTPUT_DIR = Path("data/raw/ico/specialized-reports")
USER_AGENT = "coffee-market-rag/0.1 (+https://github.com/)"


def build_ssl_context(insecure: bool) -> ssl.SSLContext | None:
    if not insecure:
        return None
    return ssl._create_unverified_context()


def fetch_text(url: str, timeout: int, insecure: bool) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=timeout, context=build_ssl_context(insecure)) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def download_file(url: str, destination: Path, timeout: int, insecure: bool) -> None:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=timeout, context=build_ssl_context(insecure)) as response:
        destination.write_bytes(response.read())


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    cleaned_path = parsed.path or "/"
    return urlunparse((parsed.scheme, parsed.netloc, cleaned_path, "", "", ""))


def filename_from_url(url: str) -> str:
    path_name = Path(urlparse(url).path).name
    if path_name:
        return path_name
    return "download.pdf"


@dataclass(frozen=True)
class ReportLink:
    title: str
    url: str
    filename: str
    section: str | None = None


class SpecializedReportsParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links: list[ReportLink] = []
        self._section_stack: list[str] = []
        self._capture_heading = False
        self._heading_buffer: list[str] = []
        self._in_anchor = False
        self._anchor_href: str | None = None
        self._anchor_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)

        if tag in {"h1", "h2", "h3", "h4"}:
            self._capture_heading = True
            self._heading_buffer = []
            return

        if tag == "a":
            self._in_anchor = True
            self._anchor_href = attrs_dict.get("href")
            self._anchor_text = []

    def handle_data(self, data: str) -> None:
        if self._capture_heading:
            self._heading_buffer.append(data)
        if self._in_anchor:
            self._anchor_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"h1", "h2", "h3", "h4"} and self._capture_heading:
            heading = clean_text("".join(self._heading_buffer))
            self._capture_heading = False
            self._heading_buffer = []
            if heading:
                self._section_stack = [heading]
            return

        if tag == "a" and self._in_anchor:
            href = self._anchor_href
            text = clean_text("".join(self._anchor_text))
            self._in_anchor = False
            self._anchor_href = None
            self._anchor_text = []

            if not href:
                return

            absolute_url = normalize_url(urljoin(self.base_url, href))
            if not absolute_url.lower().endswith(".pdf"):
                return

            self.links.append(
                ReportLink(
                    title=text or filename_from_url(absolute_url),
                    url=absolute_url,
                    filename=filename_from_url(absolute_url),
                    section=self._section_stack[-1] if self._section_stack else None,
                )
            )


def extract_pdf_links(html: str, base_url: str) -> list[ReportLink]:
    parser = SpecializedReportsParser(base_url)
    parser.feed(html)

    deduped: dict[str, ReportLink] = {}
    for link in parser.links:
        deduped.setdefault(link.url, link)

    return list(deduped.values())


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_manifest(reports: Iterable[ReportLink], manifest_path: Path, source_url: str) -> None:
    ensure_directory(manifest_path.parent)
    payload = {
        "source_url": source_url,
        "report_count": len(list(reports)),
        "reports": [asdict(report) for report in reports],
    }
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL, help="ICO page to scrape")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for downloaded PDFs",
    )
    parser.add_argument(
        "--manifest-path",
        default=str(DEFAULT_OUTPUT_DIR / "reports.json"),
        help="Path for the JSON manifest",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download discovered PDFs to the output directory",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit number of PDFs")
    parser.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification for local/dev environments",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    manifest_path = Path(args.manifest_path)

    try:
        html = fetch_text(args.url, timeout=args.timeout, insecure=args.insecure)
        reports = extract_pdf_links(html, args.url)
    except (HTTPError, URLError) as exc:
        print(f"Failed to fetch ICO page: {exc}", file=sys.stderr)
        return 1

    if args.limit is not None:
        reports = reports[: args.limit]

    write_manifest(reports, manifest_path, args.url)
    print(f"Discovered {len(reports)} PDF links")
    print(f"Manifest written to {manifest_path}")

    if not args.download:
        return 0

    ensure_directory(output_dir)
    failures = 0

    for report in reports:
        destination = output_dir / report.filename
        try:
            download_file(report.url, destination, timeout=args.timeout, insecure=args.insecure)
            print(f"Downloaded {destination}")
        except (HTTPError, URLError) as exc:
            failures += 1
            print(f"Failed to download {report.url}: {exc}", file=sys.stderr)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
