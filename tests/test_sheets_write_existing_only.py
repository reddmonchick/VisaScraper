from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from visascraper.services.sheets import GoogleSheetsManager
from visascraper.utils.sheets_rotator import ExistingSpreadsheetRequiredError


class GoogleSheetsExistingOnlyTests(unittest.TestCase):
    @patch.object(GoogleSheetsManager, "_init_client")
    @patch(
        "visascraper.services.sheets.ensure_valid_spreadsheet",
        side_effect=ExistingSpreadsheetRequiredError("manual action required"),
    )
    def test_write_to_sheet_skips_when_existing_sheet_is_required(
        self,
        _mock_ensure_valid_spreadsheet: Mock,
        mock_init_client: Mock,
    ) -> None:
        client = Mock()
        mock_init_client.return_value = client
        manager = GoogleSheetsManager()

        manager.write_to_sheet([["app", "acc-1"]], [["mgr", "acc-1"]], [["stay", "acc-1"]])

        client.open_by_key.assert_not_called()


if __name__ == "__main__":
    unittest.main()
