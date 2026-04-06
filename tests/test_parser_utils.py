from __future__ import annotations

from pathlib import Path
import sys
import unittest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from visascraper.utils.parser import (
    extract_action_link,
    extract_detail,
    extract_status,
    extract_status_batch,
    extract_visa,
    safe_get,
)


class ParserUtilsTests(unittest.TestCase):
    def test_safe_get_returns_default_for_none(self) -> None:
        self.assertEqual(safe_get({"value": None}, "value", "fallback"), "fallback")

    def test_extract_status_functions_return_span_text(self) -> None:
        html = '<span class="badge">Approved</span>'
        self.assertEqual(extract_status(html), "Approved")
        self.assertEqual(extract_status_batch(html), "Approved")

    def test_extract_links(self) -> None:
        action_html = '<a class="btn btn-sm btn-outline-info" href="/download/file.pdf">Download</a>'
        detail_html = '<a class="btn btn-sm btn-primary" href="/details/123">Details</a>'
        visa_html = '<a class="fw-bold btn btn-sm btn-outline-info btn-back" href="/visa/print">Print</a>'

        self.assertEqual(extract_action_link(action_html), "/download/file.pdf")
        self.assertEqual(extract_detail(detail_html), "/details/123")
        self.assertEqual(extract_visa(visa_html), "/visa/print")


if __name__ == "__main__":
    unittest.main()
