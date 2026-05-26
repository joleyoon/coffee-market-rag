import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from app.app import LiveState, SearchSnapshot, make_handler, refresh_live_state


def snapshot(report_count: int) -> SearchSnapshot:
    return SearchSnapshot(
        index={"chunks": []},
        metrics={"report_count": report_count, "chunk_count": 0, "start_period": "n/a", "end_period": "n/a"},
        trend_data=None,
    )


class RefreshLiveStateTests(unittest.TestCase):
    @patch("app.app.load_search_snapshot")
    def test_refresh_replaces_search_snapshot_after_pipeline_succeeds(self, load_snapshot) -> None:
        state = LiveState(snapshot(1))
        load_snapshot.return_value = snapshot(2)
        pipeline_calls: list[bool] = []

        refreshed = refresh_live_state(state, Path("index.pkl"), lambda: pipeline_calls.append(True))

        self.assertTrue(refreshed)
        self.assertEqual(pipeline_calls, [True])
        self.assertEqual(state.snapshot.metrics["report_count"], 2)
        self.assertEqual(state.last_refresh["status"], "succeeded")

    def test_refresh_keeps_existing_snapshot_when_pipeline_fails(self) -> None:
        state = LiveState(snapshot(1))

        def failed_pipeline() -> None:
            raise RuntimeError("network unavailable")

        refreshed = refresh_live_state(state, Path("index.pkl"), failed_pipeline)

        self.assertFalse(refreshed)
        self.assertEqual(state.snapshot.metrics["report_count"], 1)
        self.assertEqual(state.last_refresh["status"], "failed")


class HomepageRefreshTests(unittest.TestCase):
    @patch("app.app.refresh_live_state")
    def test_homepage_visit_triggers_a_refresh(self, refresh_state) -> None:
        state = LiveState(snapshot(1))
        handler = make_handler(state, Path("index.pkl"), top_k=5, max_sentences=4)
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        try:
            connection = HTTPConnection("127.0.0.1", server.server_port)
            connection.request("GET", "/")
            response = connection.getresponse()
            response.read()
            connection.close()
        finally:
            server_thread.join(timeout=2)
            server.server_close()

        self.assertEqual(response.status, 200)
        self.assertEqual(response.getheader("Cache-Control"), "no-store")
        refresh_state.assert_called_once_with(state, Path("index.pkl"))


if __name__ == "__main__":
    unittest.main()
