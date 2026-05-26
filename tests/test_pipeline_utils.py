import unittest

from scripts.pipeline_utils import classify_report_period, extract_coffee_type_tags, extract_country_tags


class PipelineUtilsTests(unittest.TestCase):
    def test_extract_country_tags_normalizes_aliases(self) -> None:
        tags = extract_country_tags(
            "Brazilian arabica output improved while Vietnam exports rose and Ivory Coast shipments softened."
        )
        self.assertEqual(tags, ["Brazil", "Vietnam", "Cote d'Ivoire"])

    def test_extract_coffee_type_tags_detects_arabica_and_robusta(self) -> None:
        tags = extract_coffee_type_tags(
            "Arabica prices fell while Colombian Milds stabilized and robusta exports accelerated."
        )
        self.assertEqual(tags, ["Arabica", "Robusta"])

    def test_classify_report_period_marks_latest_report(self) -> None:
        self.assertEqual(classify_report_period("2026-02-01", "2026-02-01"), "latest")
        self.assertEqual(classify_report_period("2025-12-01", "2026-02-01"), "historical")


if __name__ == "__main__":
    unittest.main()
