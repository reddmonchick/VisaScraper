from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from visascraper.config import settings
from visascraper.jobs import JobScheduler
from visascraper.services.scraper import DataParser, STAY_PERMIT_DATA_URL


class FakePdfManager:
    def upload_stay_pdf(self, session, session_id, pdf_relative_url, reg_number) -> str:
        return f"https://files.local/{reg_number}.pdf"


class FakeStayResponse:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self.status_code = 200
        self._rows = rows

    def json(self) -> dict[str, list[dict[str, str]]]:
        return {"data": self._rows}


class FakeStaySession:
    def __init__(self) -> None:
        self.starts: list[str] = []

    def get(self, url: str, **kwargs):
        if url != STAY_PERMIT_DATA_URL:
            raise AssertionError(f"Unexpected url: {url}")

        start_value = kwargs["params"]["start"]
        self.starts.append(start_value)

        if len(self.starts) == 1:
            return FakeStayResponse(
                [
                    {
                        "register_number": "REG-001",
                        "full_name": "John Doe",
                        "type_of_staypermit": "Permit A",
                        "type_of_visa": "Visa A",
                        "passport_number": "P-001",
                        "start_date": "2026-01-01",
                        "issue_date": "2026-01-02",
                        "expired_date": "2026-06-01",
                        "status": "<span>Approved</span>",
                        "action": '<a class="btn btn-sm btn-outline-info" href="/pdf/1"></a>',
                    },
                    {
                        "register_number": "REG-002",
                        "full_name": "Jane Doe",
                        "type_of_staypermit": "Permit B",
                        "type_of_visa": "Visa B",
                        "passport_number": "P-002",
                        "start_date": "2026-02-01",
                        "issue_date": "2026-02-02",
                        "expired_date": "2026-07-01",
                        "status": "<span>Approved</span>",
                        "action": '<a class="btn btn-sm btn-outline-info" href="/pdf/2"></a>',
                    },
                ]
            )

        raise RuntimeError("timed out")


class StayRetryTests(unittest.TestCase):
    def test_partial_stay_items_are_saved_after_final_retry_failure(self) -> None:
        parser = DataParser(session_manager=MagicMock(), pdf_manager=FakePdfManager())
        session = FakeStaySession()
        stored: dict[str, object] = {}

        def fake_store(account_name, parsed_items):
            stored["account_name"] = account_name
            stored["parsed_items"] = parsed_items
            return [item.to_sheet_row() for item in parsed_items]

        parser._store_stay_items = fake_store  # type: ignore[method-assign]

        with patch("visascraper.services.scraper.time.sleep"):
            rows = parser.fetch_and_update_stay(session, "ALPHA VISA", "session-1")

        self.assertEqual(session.starts, ["0", "2", "2", "2"])
        self.assertEqual(stored["account_name"], "ALPHA VISA")
        parsed_items = stored["parsed_items"]
        self.assertEqual(len(parsed_items), 2)
        self.assertEqual([item.reg_number for item in parsed_items], ["REG-001", "REG-002"])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][-1], "ALPHA VISA")
        self.assertEqual(rows[1][-1], "ALPHA VISA")


class JobSchedulerTests(unittest.TestCase):
    def test_start_scheduler_uses_single_interval_job_with_immediate_start(self) -> None:
        scheduler = JobScheduler(gs_manager=MagicMock(), data_parser=MagicMock())
        scheduler.scheduler = MagicMock()

        scheduler.start_scheduler()

        scheduler.scheduler.add_job.assert_called_once()
        scheduler.scheduler.start.assert_called_once()

        args, kwargs = scheduler.scheduler.add_job.call_args
        self.assertEqual(args[1], "interval")
        self.assertEqual(kwargs["id"], "priority_accounts_interval")
        self.assertEqual(kwargs["minutes"], settings.batch_parse_interval_minutes)
        self.assertEqual(kwargs["misfire_grace_time"], 300)
        self.assertEqual(kwargs["max_instances"], 1)
        self.assertTrue(kwargs["coalesce"])
        self.assertIsNotNone(kwargs["next_run_time"])
        self.assertEqual(getattr(kwargs["next_run_time"].tzinfo, "key", None), settings.app_timezone)


if __name__ == "__main__":
    unittest.main()
