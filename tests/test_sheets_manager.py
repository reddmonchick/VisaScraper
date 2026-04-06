from __future__ import annotations

from pathlib import Path
import sys
import unittest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from visascraper.services.sheets import GoogleSheetsManager


class GoogleSheetsManagerTests(unittest.TestCase):
    def test_compose_final_rows_preserves_other_accounts(self) -> None:
        existing_rows = [
            ["Name", "Account"],
            ["Alice", "keep-me"],
            ["Bob", "replace-me"],
        ]
        incoming_rows = [["Charlie", "replace-me"]]

        final_rows = GoogleSheetsManager._compose_final_rows(
            existing_rows=existing_rows,
            header=["Name", "Account"],
            incoming_rows=incoming_rows,
            preserve_account_index=1,
            accounts_to_replace={"replace-me"},
        )

        self.assertEqual(
            final_rows,
            [
                ["Name", "Account"],
                ["Alice", "keep-me"],
                ["Charlie", "replace-me"],
            ],
        )

    def test_compose_final_rows_normalizes_none_values(self) -> None:
        final_rows = GoogleSheetsManager._compose_final_rows(
            existing_rows=[["Name", "Account"]],
            header=["Name", "Account"],
            incoming_rows=[[None, "acc-1"]],
            preserve_account_index=1,
            accounts_to_replace={"acc-1"},
        )

        self.assertEqual(final_rows[1], ["", "acc-1"])


if __name__ == "__main__":
    unittest.main()
