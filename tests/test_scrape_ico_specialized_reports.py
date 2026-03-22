import unittest

from scripts.scrape_ico_specialized_reports import extract_pdf_links


SAMPLE_HTML = """
<html>
  <body>
    <h2>Coffee Market Reports</h2>
    <ul>
      <li><a href="/wp-content/uploads/2026/02/report-jan-2026.pdf">Monthly Coffee Market Report - January 2026</a></li>
      <li><a href="https://ico.org/wp-content/uploads/2025/12/report-dec-2025.pdf">Monthly Coffee Market Report - December 2025</a></li>
      <li><a href="/specialized-reports/">Landing Page</a></li>
    </ul>
    <h2>Other Reports</h2>
    <p><a href="/docs/coffee-break.pdf?download=1">Coffee Break</a></p>
    <p><a href="/docs/coffee-break.pdf">Coffee Break Duplicate</a></p>
  </body>
</html>
"""


class ExtractPdfLinksTests(unittest.TestCase):
    def test_extracts_relative_and_absolute_pdf_links(self) -> None:
        reports = extract_pdf_links(SAMPLE_HTML, "https://ico.org/specialized-reports/")

        self.assertEqual(len(reports), 3)
        self.assertEqual(reports[0].section, "Coffee Market Reports")
        self.assertEqual(reports[0].filename, "report-jan-2026.pdf")
        self.assertEqual(reports[2].section, "Other Reports")
        self.assertEqual(reports[2].url, "https://ico.org/docs/coffee-break.pdf")


if __name__ == "__main__":
    unittest.main()
