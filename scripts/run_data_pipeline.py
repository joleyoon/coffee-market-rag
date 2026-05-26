#!/usr/bin/env python3
"""Run the end-to-end ingest -> clean -> chunk -> embed -> store pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import build_vector_index, chunk_reports, extract_report_text
from scripts.export_static_search_data import export_static_search_data
from scripts.export_trend_data import export_trend_data
from scripts.pipeline_utils import (
    DEFAULT_PIPELINE_MANIFEST,
    DEFAULT_PROCESSED_ROOT,
    DEFAULT_STATIC_SEARCH_DATA,
    DEFAULT_VERSIONS_ROOT,
    build_dataset_version,
    build_version_paths,
    iso_timestamp,
    latest_manifest,
    publish_latest_aliases,
)
from scripts.report_utils import ensure_directory, write_json
from scripts.scrape_ico_specialized_reports import (
    DEFAULT_OUTPUT_DIR as DEFAULT_RAW_OUTPUT_DIR,
    DEFAULT_SECTION,
    DEFAULT_URL,
    ReportLink,
    download_file,
    extract_pdf_links,
    fetch_text,
    filter_reports_by_section,
    write_manifest,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--raw-output-dir", default=str(DEFAULT_RAW_OUTPUT_DIR))
    parser.add_argument("--manifest-path", default=str(DEFAULT_RAW_OUTPUT_DIR / "reports.json"))
    parser.add_argument("--section", default=DEFAULT_SECTION)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--skip-ingest", action="store_true")
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download PDFs even when the file already exists locally.",
    )
    parser.add_argument("--version", default=None)
    parser.add_argument("--processed-root", default=str(DEFAULT_PROCESSED_ROOT))
    parser.add_argument("--versions-root", default=str(DEFAULT_VERSIONS_ROOT))
    parser.add_argument("--latest-manifest-path", default=str(DEFAULT_PIPELINE_MANIFEST))
    parser.add_argument("--chunk-size", type=int, default=400)
    parser.add_argument("--overlap", type=int, default=80)
    parser.add_argument("--max-features", type=int, default=50000)
    parser.add_argument("--skip-trend-export", action="store_true")
    parser.add_argument("--skip-static-export", action="store_true")
    parser.add_argument("--static-output-path", default=str(DEFAULT_STATIC_SEARCH_DATA))
    parser.add_argument("--skip-publish-latest", action="store_true")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def report_ids_from_manifest(manifest: dict | None) -> set[str]:
    if not manifest:
        return set()
    return {report["report_id"] for report in manifest.get("reports", [])}


def ingest_reports(
    url: str,
    raw_output_dir: Path,
    manifest_path: Path,
    section: str | None,
    limit: int | None,
    timeout: int,
    insecure: bool,
    force_download: bool,
) -> tuple[list[ReportLink], int]:
    html = fetch_text(url, timeout=timeout, insecure=insecure)
    reports = filter_reports_by_section(extract_pdf_links(html, url), section)
    if limit is not None:
        reports = reports[:limit]

    ensure_directory(raw_output_dir)
    downloaded_reports: list[ReportLink] = []
    failures = 0

    print(f"[ingest] discovered {len(reports)} reports")
    for report in reports:
        destination = raw_output_dir / report.filename
        if destination.exists() and not force_download:
            downloaded_reports.append(
                ReportLink(
                    title=report.title,
                    url=report.url,
                    filename=report.filename,
                    section=report.section,
                    local_path=str(destination),
                )
            )
            print(f"[ingest] reused {destination}")
            continue

        try:
            download_file(report.url, destination, timeout=timeout, insecure=insecure)
            downloaded_reports.append(
                ReportLink(
                    title=report.title,
                    url=report.url,
                    filename=report.filename,
                    section=report.section,
                    local_path=str(destination),
                )
            )
            print(f"[ingest] downloaded {destination}")
        except (HTTPError, URLError) as exc:
            failures += 1
            downloaded_reports.append(report)
            print(f"[ingest] failed to download {report.url}: {exc}", file=sys.stderr)

    write_manifest(downloaded_reports, manifest_path, url)
    print(f"[ingest] manifest written to {manifest_path}")
    return downloaded_reports, failures


def build_pipeline_manifest(
    dataset_version: str,
    built_at: str,
    source_manifest_path: Path,
    version_root: Path,
    extracted_reports: list[dict],
    chunks: list[dict],
    index_payload: dict,
    previous_manifest: dict | None,
) -> dict:
    previous_report_ids = report_ids_from_manifest(previous_manifest)
    latest_report_date = max(
        (report["published_date"] for report in extracted_reports if report.get("published_date")),
        default=None,
    )
    current_report_ids = {report["report_id"] for report in extracted_reports}

    return {
        "dataset_version": dataset_version,
        "built_at": built_at,
        "stages": ["ingest", "clean", "chunk", "embed", "store", "serve"],
        "embedding_backend": index_payload.get("metadata", {}).get("embedding_backend"),
        "source_manifest_path": str(source_manifest_path),
        "version_root": str(version_root),
        "artifacts": {
            "extracted_jsonl": str(version_root / "extracted_text" / "reports.jsonl"),
            "chunks_path": str(version_root / "chunks" / "chunks.jsonl"),
            "index_path": str(version_root / "index" / "tfidf_index.pkl"),
            "trend_path": str(version_root / "trends" / "trend-data.json"),
        },
        "report_count": len(current_report_ids),
        "chunk_count": len(chunks),
        "latest_report_date": latest_report_date,
        "new_report_ids": sorted(current_report_ids - previous_report_ids),
        "existing_report_ids": sorted(current_report_ids & previous_report_ids),
        "countries": sorted({tag for report in extracted_reports for tag in report.get("country_tags", [])}),
        "coffee_types": sorted({tag for report in extracted_reports for tag in report.get("coffee_type_tags", [])}),
        "reports": [
            {
                "report_id": report["report_id"],
                "title": report["title"],
                "published_date": report.get("published_date"),
                "report_period": report.get("report_period"),
                "ingest_status": report.get("ingest_status"),
                "dataset_version": report.get("dataset_version"),
                "country_tags": report.get("country_tags", []),
                "coffee_type_tags": report.get("coffee_type_tags", []),
            }
            for report in sorted(
                extracted_reports,
                key=lambda item: (item.get("published_date") or "", item["report_id"]),
                reverse=True,
            )
        ],
    }


def run_pipeline(args: argparse.Namespace) -> tuple[dict, int]:
    dataset_version = args.version or build_dataset_version()
    built_at = iso_timestamp()
    processed_root = Path(args.processed_root)
    versions_root = Path(args.versions_root)
    version_paths = build_version_paths(dataset_version, versions_root=versions_root)
    raw_output_dir = Path(args.raw_output_dir)
    manifest_path = Path(args.manifest_path)
    previous_manifest = latest_manifest(Path(args.latest_manifest_path))
    previous_report_ids = report_ids_from_manifest(previous_manifest)

    ensure_directory(version_paths.root)

    failures = 0
    if not args.skip_ingest:
        _, failures = ingest_reports(
            url=args.url,
            raw_output_dir=raw_output_dir,
            manifest_path=manifest_path,
            section=args.section,
            limit=args.limit,
            timeout=args.timeout,
            insecure=args.insecure,
            force_download=args.force_download,
        )
    else:
        print(f"[ingest] skipped, using {manifest_path}")

    print("[clean] extracting report text")
    extracted_reports = extract_report_text.extract_reports(
        manifest_path=manifest_path,
        output_dir=version_paths.extracted_dir,
        jsonl_path=version_paths.extracted_jsonl,
        dataset_version=dataset_version,
        ingested_at=built_at,
        previous_report_ids=previous_report_ids,
    )
    if not extracted_reports:
        raise ValueError(f"No reports were extracted from {manifest_path}")

    print("[chunk] creating retrieval chunks")
    chunks = chunk_reports.chunk_report_records(
        extracted_reports,
        chunk_size=args.chunk_size,
        overlap=args.overlap,
        dataset_version=dataset_version,
    )
    if not chunks:
        raise ValueError("Chunking produced zero chunks")
    chunk_reports.write_chunk_records(chunks, version_paths.chunks_path)
    print(f"[chunk] wrote {len(chunks)} chunks to {version_paths.chunks_path}")

    print("[embed] building retrieval index")
    index_payload = build_vector_index.build_index(
        chunks,
        output_path=version_paths.index_path,
        max_features=args.max_features,
        dataset_version=dataset_version,
        built_at=built_at,
    )

    if not args.skip_trend_export:
        try:
            print("[store] exporting trend data")
            export_trend_data(version_paths.extracted_dir, version_paths.trend_path)
        except (FileNotFoundError, ValueError) as exc:
            print(f"[store] skipped trend export: {exc}", file=sys.stderr)

    pipeline_manifest = build_pipeline_manifest(
        dataset_version=dataset_version,
        built_at=built_at,
        source_manifest_path=manifest_path,
        version_root=version_paths.root,
        extracted_reports=extracted_reports,
        chunks=chunks,
        index_payload=index_payload,
        previous_manifest=previous_manifest,
    )
    write_json(version_paths.pipeline_manifest_path, pipeline_manifest)
    print(f"[store] wrote pipeline manifest to {version_paths.pipeline_manifest_path}")

    if not args.skip_publish_latest:
        publish_latest_aliases(version_paths, processed_root=processed_root)
        print(f"[store] published latest aliases under {processed_root}")

    if not args.skip_static_export:
        export_static_search_data(
            input_path=version_paths.chunks_path,
            output_path=Path(args.static_output_path),
            trend_path=version_paths.trend_path,
            pipeline_manifest_path=version_paths.pipeline_manifest_path,
        )

    return pipeline_manifest, failures


def main() -> int:
    args = parse_args()
    try:
        pipeline_manifest, failures = run_pipeline(args)
    except (HTTPError, URLError, ValueError) as exc:
        print(f"[pipeline] failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"[serve] dataset {pipeline_manifest['dataset_version']} ready with "
        f"{pipeline_manifest['report_count']} reports and {pipeline_manifest['chunk_count']} chunks"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
