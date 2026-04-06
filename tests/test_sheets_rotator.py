from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from visascraper.utils.sheets_rotator import ExistingSpreadsheetRequiredError, ensure_valid_spreadsheet


class _Worksheet:
    def __init__(self, row_count: int, col_count: int) -> None:
        self.row_count = row_count
        self.col_count = col_count


class _Spreadsheet:
    def __init__(self, worksheets: list[_Worksheet]) -> None:
        self._worksheets = worksheets

    def worksheets(self) -> list[_Worksheet]:
        return self._worksheets


class SheetsRotatorTests(unittest.TestCase):
    @patch("visascraper.utils.sheets_rotator.get_current_data_sheet_id", return_value="")
    def test_ensure_valid_spreadsheet_requires_existing_active_sheet(self, _mock_get_sheet_id: Mock) -> None:
        with self.assertRaises(ExistingSpreadsheetRequiredError):
            ensure_valid_spreadsheet(Mock())

    @patch("visascraper.utils.sheets_rotator.get_current_data_sheet_id", return_value="sheet-123")
    def test_ensure_valid_spreadsheet_requires_accessible_sheet(self, _mock_get_sheet_id: Mock) -> None:
        client = Mock()
        client.open_by_key.side_effect = RuntimeError("403")

        with self.assertRaises(ExistingSpreadsheetRequiredError):
            ensure_valid_spreadsheet(client)

    @patch("visascraper.utils.sheets_rotator.get_current_data_sheet_id", return_value="sheet-123")
    def test_ensure_valid_spreadsheet_rejects_oversized_sheet(self, _mock_get_sheet_id: Mock) -> None:
        client = Mock()
        client.open_by_key.return_value = _Spreadsheet([_Worksheet(100_000, 100)])

        with self.assertRaises(ExistingSpreadsheetRequiredError):
            ensure_valid_spreadsheet(client)


if __name__ == "__main__":
    unittest.main()
