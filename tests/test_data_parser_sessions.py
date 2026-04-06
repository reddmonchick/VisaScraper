from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from visascraper.services.scraper import DataParser


class FakeSession:
    pass


class FakeSessionManager:
    def __init__(self) -> None:
        self.created_sessions: list[FakeSession] = []
        self.closed_sessions: list[FakeSession] = []

    def create_session(self) -> FakeSession:
        session = FakeSession()
        self.created_sessions.append(session)
        return session

    def close_session(self, session: FakeSession) -> None:
        self.closed_sessions.append(session)


class FakePdfManager:
    pass


class DataParserSessionTests(unittest.TestCase):
    def test_single_session_is_reused_for_account_requests(self) -> None:
        session_manager = FakeSessionManager()
        parser = DataParser(session_manager=session_manager, pdf_manager=FakePdfManager())
        seen_sessions: list[FakeSession] = []

        def fake_fetch_stay(session, account_name, session_id):
            seen_sessions.append(session)
            self.assertEqual(account_name, "acc-1")
            self.assertEqual(session_id, "session-1")
            return []

        def fake_fetch_batch(session, account_name, session_id):
            seen_sessions.append(session)
            self.assertEqual(account_name, "acc-1")
            self.assertEqual(session_id, "session-1")
            return [], []

        parser.fetch_and_update_stay = fake_fetch_stay
        parser.fetch_and_update_batch = fake_fetch_batch

        with (
            patch("visascraper.services.scraper.load_session", return_value="session-1"),
            patch("visascraper.services.scraper.check_session", return_value=True),
        ):
            parser.parse_accounts(["acc-1"], ["pwd-1"])

        self.assertEqual(len(session_manager.created_sessions), 1)
        self.assertEqual(session_manager.closed_sessions, session_manager.created_sessions)
        self.assertEqual(seen_sessions, [session_manager.created_sessions[0], session_manager.created_sessions[0]])

    def test_failed_login_closes_session_and_skips_fetches(self) -> None:
        session_manager = FakeSessionManager()
        parser = DataParser(session_manager=session_manager, pdf_manager=FakePdfManager())
        fetch_called = False

        def fake_fetch(*args, **kwargs):
            nonlocal fetch_called
            fetch_called = True
            return []

        parser.fetch_and_update_stay = fake_fetch
        parser.fetch_and_update_batch = fake_fetch

        with (
            patch("visascraper.services.scraper.load_session", return_value=None),
            patch("visascraper.services.scraper.check_session", return_value=False),
            patch("visascraper.services.scraper.login", return_value=None),
        ):
            parser.parse_accounts(["acc-1"], ["pwd-1"])

        self.assertFalse(fetch_called)
        self.assertEqual(session_manager.closed_sessions, session_manager.created_sessions)


if __name__ == "__main__":
    unittest.main()
