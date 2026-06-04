import unittest
from unittest.mock import patch

from app.app import LLMConfig, answer_query, parse_llm_answer


def make_result() -> dict:
    return {
        "report_id": "cmr-0226-e",
        "chunk_id": "cmr-0226-e-p001-c01",
        "title": "Monthly Coffee Market Report - February 2026",
        "source_url": "https://example.com/report.pdf",
        "published_date": "2026-02-01",
        "page_number": 1,
        "country_tags": ["Brazil"],
        "coffee_type_tags": ["Arabica"],
        "dataset_version": "20260328T000000Z",
        "score": 0.91,
        "chunk_text": (
            "Improved supply outlooks, including strong production forecasts for Brazil, "
            "weighed on coffee prices in February 2026."
        ),
    }


class LlmGenerationTests(unittest.TestCase):
    def test_parse_llm_answer_accepts_json_fenced_output(self) -> None:
        answer, why, sources = parse_llm_answer(
            '```json\n{"answer": "Brazil supply pressure weighed on prices.", '
            '"why": ["The retrieved chunk cites stronger Brazil production forecasts."], '
            '"sources": ["S1"]}\n```'
        )

        self.assertEqual(answer, "Brazil supply pressure weighed on prices.")
        self.assertEqual(why, ["The retrieved chunk cites stronger Brazil production forecasts."])
        self.assertEqual(sources, ["S1"])

    def test_answer_query_uses_llm_when_configured(self) -> None:
        config = LLMConfig(
            mode="auto",
            model="test-model",
            api_key="test-key",
            base_url="https://example.com/v1/responses",
            timeout=1,
            max_output_tokens=200,
        )
        response_payload = {
            "output": [
                {
                    "content": [
                        {
                            "type": "output_text",
                            "text": (
                                '{"answer": "The LLM grounded answer.", '
                                '"why": ["It used the retrieved evidence."], '
                                '"sources": ["S1"]}'
                            ),
                        }
                    ]
                }
            ]
        }

        with patch("app.app.search_index", return_value=[make_result()]):
            with patch("app.app.call_openai_responses_api", return_value=response_payload) as mocked_call:
                payload = answer_query(
                    {"metadata": {"dataset_version": "20260328T000000Z"}},
                    "What moved prices?",
                    top_k=1,
                    max_sentences=2,
                    llm_config=config,
                )

        mocked_call.assert_called_once()
        self.assertEqual(payload["answer_mode"], "llm")
        self.assertEqual(payload["llm_model"], "test-model")
        self.assertEqual(payload["answer"], "The LLM grounded answer.")
        self.assertEqual(payload["why"], ["It used the retrieved evidence."])
        self.assertEqual(payload["sources"][0]["report_id"], "cmr-0226-e")

    def test_answer_query_falls_back_without_api_key(self) -> None:
        config = LLMConfig(
            mode="auto",
            model="test-model",
            api_key=None,
            base_url="https://example.com/v1/responses",
            timeout=1,
            max_output_tokens=200,
        )

        with patch("app.app.search_index", return_value=[make_result()]):
            with patch("app.app.call_openai_responses_api") as mocked_call:
                payload = answer_query(
                    {"metadata": {"dataset_version": "20260328T000000Z"}},
                    "What moved prices?",
                    top_k=1,
                    max_sentences=2,
                    llm_config=config,
                )

        mocked_call.assert_not_called()
        self.assertEqual(payload["answer_mode"], "extractive")
        self.assertIsNone(payload["llm_model"])
        self.assertIn("Brazil", payload["answer"])

    def test_answer_query_required_mode_errors_without_api_key(self) -> None:
        config = LLMConfig(
            mode="required",
            model="test-model",
            api_key=None,
            base_url="https://example.com/v1/responses",
            timeout=1,
            max_output_tokens=200,
        )

        with patch("app.app.search_index", return_value=[make_result()]):
            with self.assertRaises(RuntimeError):
                answer_query(
                    {"metadata": {"dataset_version": "20260328T000000Z"}},
                    "What moved prices?",
                    top_k=1,
                    max_sentences=2,
                    llm_config=config,
                )


if __name__ == "__main__":
    unittest.main()
