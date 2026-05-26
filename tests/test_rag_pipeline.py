import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import build_vector_index, chunk_reports, run_data_pipeline
from scripts.query_index import load_index, search_index
from scripts.report_utils import load_jsonl, write_json, write_jsonl


def make_chunk(
    report_id: str,
    chunk_id: str,
    chunk_text: str,
    page_number: int = 1,
    title: str = "Monthly Coffee Market Report - February 2026",
    published_date: str = "2026-02-01",
    country_tags: list[str] | None = None,
    coffee_type_tags: list[str] | None = None,
    dataset_version: str = "20260328T000000Z",
) -> dict:
    return {
        "report_id": report_id,
        "chunk_id": chunk_id,
        "title": title,
        "source_url": "https://example.com/report.pdf",
        "published_date": published_date,
        "report_month": published_date[:7],
        "report_year": published_date[:4],
        "page_number": page_number,
        "chunk_index": 1,
        "dataset_version": dataset_version,
        "country_tags": country_tags or [],
        "coffee_type_tags": coffee_type_tags or [],
        "report_period": "latest",
        "ingest_status": "new",
        "chunk_text": chunk_text,
    }


class RagPipelineTests(unittest.TestCase):
    def test_chunk_reports_generates_expected_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_path = tmp_path / "reports.jsonl"
            output_path = tmp_path / "chunks.jsonl"
            write_jsonl(
                input_path,
                [
                    {
                        "report_id": "cmr-0226-e",
                        "title": "Monthly Coffee Market Report - February 2026",
                        "source_url": "https://example.com/report.pdf",
                        "published_date": "2026-02-01",
                        "report_month": "2026-02",
                        "report_year": "2026",
                        "dataset_version": "20260328T000000Z",
                        "report_period": "latest",
                        "ingest_status": "new",
                        "pages": [
                            {
                                "page_number": 1,
                                "text": "Brazil arabica one two three four five six",
                            }
                        ],
                    }
                ],
            )

            with patch.object(
                sys,
                "argv",
                [
                    "chunk_reports.py",
                    "--input-path",
                    str(input_path),
                    "--output-path",
                    str(output_path),
                    "--chunk-size",
                    "3",
                    "--overlap",
                    "1",
                ],
            ):
                with redirect_stdout(io.StringIO()):
                    exit_code = chunk_reports.main()

            self.assertEqual(exit_code, 0)

            chunks = load_jsonl(output_path)
            self.assertEqual(len(chunks), 4)
            self.assertEqual(
                [chunk["chunk_text"] for chunk in chunks],
                ["Brazil arabica one", "one two three", "three four five", "five six"],
            )
            self.assertEqual(
                [chunk["chunk_id"] for chunk in chunks],
                [
                    "cmr-0226-e-p001-c01",
                    "cmr-0226-e-p001-c02",
                    "cmr-0226-e-p001-c03",
                    "cmr-0226-e-p001-c04",
                ],
            )
            self.assertEqual([chunk["chunk_index"] for chunk in chunks], [1, 2, 3, 4])
            self.assertEqual(chunks[0]["country_tags"], ["Brazil"])
            self.assertEqual(chunks[0]["coffee_type_tags"], ["Arabica"])
            self.assertEqual(chunks[0]["dataset_version"], "20260328T000000Z")
            self.assertEqual(chunks[0]["report_period"], "latest")

    def test_build_vector_index_and_search_rank_relevant_chunk_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            chunks_path = tmp_path / "chunks.jsonl"
            index_path = tmp_path / "tfidf_index.pkl"
            write_jsonl(
                chunks_path,
                [
                    make_chunk(
                        report_id="cmr-0226-e",
                        chunk_id="cmr-0226-e-p001-c01",
                        chunk_text="Brazil rainfall outlook improved after steady rain lifted arabica supply expectations.",
                        country_tags=["Brazil"],
                        coffee_type_tags=["Arabica"],
                    ),
                    make_chunk(
                        report_id="cmr-0126-e",
                        chunk_id="cmr-0126-e-p001-c01",
                        chunk_text="Vietnam exports remained strong while robusta shipments accelerated.",
                        title="Monthly Coffee Market Report - January 2026",
                        published_date="2026-01-01",
                        country_tags=["Vietnam"],
                        coffee_type_tags=["Robusta"],
                    ),
                    make_chunk(
                        report_id="cmr-1125-e",
                        chunk_id="cmr-1125-e-p001-c01",
                        chunk_text="European inventories tightened as certified stocks declined.",
                        title="Monthly Coffee Market Report - November 2025",
                        published_date="2025-11-01",
                    ),
                ],
            )

            with patch.object(
                sys,
                "argv",
                [
                    "build_vector_index.py",
                    "--input-path",
                    str(chunks_path),
                    "--output-path",
                    str(index_path),
                    "--max-features",
                    "100",
                ],
            ):
                with redirect_stdout(io.StringIO()):
                    exit_code = build_vector_index.main()

            self.assertEqual(exit_code, 0)

            index = load_index(index_path)
            self.assertEqual(index["matrix"].shape[0], 3)

            results = search_index(index, "brazil rainfall outlook", top_k=2)

            self.assertEqual(len(results), 2)
            self.assertEqual(results[0]["report_id"], "cmr-0226-e")
            self.assertIn("Brazil rainfall outlook improved", results[0]["chunk_text"])
            self.assertGreaterEqual(results[0]["score"], results[1]["score"])

    def test_search_index_filters_by_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            chunks_path = tmp_path / "chunks.jsonl"
            index_path = tmp_path / "tfidf_index.pkl"
            write_jsonl(
                chunks_path,
                [
                    make_chunk(
                        report_id="cmr-0226-e",
                        chunk_id="cmr-0226-e-p001-c01",
                        chunk_text="Brazil rainfall outlook improved after steady rain lifted arabica supply expectations.",
                        country_tags=["Brazil"],
                        coffee_type_tags=["Arabica"],
                    ),
                    make_chunk(
                        report_id="cmr-0126-e",
                        chunk_id="cmr-0126-e-p001-c01",
                        chunk_text="Vietnam exports remained strong while robusta shipments accelerated.",
                        title="Monthly Coffee Market Report - January 2026",
                        published_date="2026-01-01",
                        country_tags=["Vietnam"],
                        coffee_type_tags=["Robusta"],
                    ),
                ],
            )

            build_vector_index.build_index(load_jsonl(chunks_path), output_path=index_path, max_features=100)
            index = load_index(index_path)

            results = search_index(
                index,
                "exports accelerated",
                top_k=5,
                filters={"countries": ["Vietnam"], "coffee_types": ["Robusta"]},
            )

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["report_id"], "cmr-0126-e")
            self.assertEqual(results[0]["country_tags"], ["Vietnam"])
            self.assertEqual(results[0]["coffee_type_tags"], ["Robusta"])

    def test_build_pipeline_manifest_tracks_new_and_existing_reports(self) -> None:
        extracted_reports = [
            {
                "report_id": "cmr-0226-e",
                "title": "Monthly Coffee Market Report - February 2026",
                "published_date": "2026-02-01",
                "report_period": "latest",
                "ingest_status": "new",
                "dataset_version": "20260328T000000Z",
                "country_tags": ["Brazil"],
                "coffee_type_tags": ["Arabica"],
            },
            {
                "report_id": "cmr-0126-e",
                "title": "Monthly Coffee Market Report - January 2026",
                "published_date": "2026-01-01",
                "report_period": "historical",
                "ingest_status": "existing",
                "dataset_version": "20260328T000000Z",
                "country_tags": ["Vietnam"],
                "coffee_type_tags": ["Robusta"],
            },
        ]
        chunks = [
            make_chunk(
                report_id="cmr-0226-e",
                chunk_id="cmr-0226-e-p001-c01",
                chunk_text="Brazil rainfall outlook improved after steady rain lifted arabica supply expectations.",
                country_tags=["Brazil"],
                coffee_type_tags=["Arabica"],
            ),
            make_chunk(
                report_id="cmr-0126-e",
                chunk_id="cmr-0126-e-p001-c01",
                chunk_text="Vietnam exports remained strong while robusta shipments accelerated.",
                title="Monthly Coffee Market Report - January 2026",
                published_date="2026-01-01",
                country_tags=["Vietnam"],
                coffee_type_tags=["Robusta"],
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source_manifest_path = tmp_path / "reports.json"
            write_json(source_manifest_path, {"reports": []})
            index_payload = build_vector_index.build_index(chunks, output_path=tmp_path / "index.pkl", max_features=100)

            manifest = run_data_pipeline.build_pipeline_manifest(
                dataset_version="20260328T000000Z",
                built_at="2026-03-28T00:00:00Z",
                source_manifest_path=source_manifest_path,
                version_root=tmp_path / "versions" / "20260328T000000Z",
                extracted_reports=extracted_reports,
                chunks=chunks,
                index_payload=index_payload,
                previous_manifest={
                    "reports": [
                        {
                            "report_id": "cmr-0126-e",
                            "title": "Monthly Coffee Market Report - January 2026",
                        }
                    ]
                },
            )

            self.assertEqual(manifest["new_report_ids"], ["cmr-0226-e"])
            self.assertEqual(manifest["existing_report_ids"], ["cmr-0126-e"])
            self.assertEqual(manifest["report_count"], 2)
            self.assertEqual(manifest["chunk_count"], 2)
            self.assertEqual(manifest["coffee_types"], ["Arabica", "Robusta"])
            self.assertEqual(manifest["reports"][0]["report_id"], "cmr-0226-e")


if __name__ == "__main__":
    unittest.main()
