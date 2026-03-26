import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import build_vector_index, chunk_reports
from scripts.query_index import load_index, search_index
from scripts.report_utils import load_jsonl, write_jsonl


def make_chunk(
    report_id: str,
    chunk_id: str,
    chunk_text: str,
    page_number: int = 1,
    title: str = "Monthly Coffee Market Report - February 2026",
) -> dict:
    return {
        "report_id": report_id,
        "chunk_id": chunk_id,
        "title": title,
        "source_url": "https://example.com/report.pdf",
        "published_date": "2026-02-01",
        "page_number": page_number,
        "chunk_index": 1,
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
                        "pages": [
                            {
                                "page_number": 1,
                                "text": "one two three four five six",
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
            self.assertEqual(len(chunks), 3)
            self.assertEqual(
                [chunk["chunk_text"] for chunk in chunks],
                ["one two three", "three four five", "five six"],
            )
            self.assertEqual(
                [chunk["chunk_id"] for chunk in chunks],
                ["cmr-0226-e-p001-c01", "cmr-0226-e-p001-c02", "cmr-0226-e-p001-c03"],
            )
            self.assertEqual([chunk["chunk_index"] for chunk in chunks], [1, 2, 3])

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
                    ),
                    make_chunk(
                        report_id="cmr-0126-e",
                        chunk_id="cmr-0126-e-p001-c01",
                        chunk_text="Vietnam exports remained strong while robusta shipments accelerated.",
                    ),
                    make_chunk(
                        report_id="cmr-1125-e",
                        chunk_id="cmr-1125-e-p001-c01",
                        chunk_text="European inventories tightened as certified stocks declined.",
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


if __name__ == "__main__":
    unittest.main()
