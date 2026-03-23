import unittest

from scripts.report_utils import chunk_words, parse_published_date, report_id_from_filename


class ReportUtilsTests(unittest.TestCase):
    def test_parse_published_date_from_title(self) -> None:
        self.assertEqual(
            parse_published_date("Monthly Coffee Market Report - February 2026", "cmr-0226-e.pdf"),
            "2026-02-01",
        )

    def test_parse_published_date_from_filename_fallback(self) -> None:
        self.assertEqual(
            parse_published_date("cmr-0925-e.pdf", "cmr-0925-e.pdf"),
            "2025-09-01",
        )

    def test_report_id_from_filename(self) -> None:
        self.assertEqual(report_id_from_filename("cmr-0226-e.pdf"), "cmr-0226-e")

    def test_chunk_words(self) -> None:
        chunks = chunk_words("one two three four five six seven eight nine ten", chunk_size=4, overlap=1)
        self.assertEqual(chunks, ["one two three four", "four five six seven", "seven eight nine ten"])


if __name__ == "__main__":
    unittest.main()
