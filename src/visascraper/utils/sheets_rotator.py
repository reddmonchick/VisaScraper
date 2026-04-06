from __future__ import annotations

import gspread

from visascraper.config import settings
from visascraper.utils.logger import logger

MAX_SPREADSHEET_CELLS = 8_500_000


class ExistingSpreadsheetRequiredError(RuntimeError):
    """Raised when there is no usable active spreadsheet and auto-creation is disabled."""


def get_current_data_sheet_id(gc: gspread.Client) -> str:
    if not settings.google_archive_index_id:
        return ""

    try:
        index_ss = gc.open_by_key(settings.google_archive_index_id)
        ws = index_ss.sheet1
        values = ws.get_all_values()
        for row in reversed(values):
            if len(row) >= 4 and "активна" in row[3].lower():
                url = row[2]
                return url.split("/d/")[-1].split("/")[0]
    except Exception as exc:
        logger.error("Ошибка чтения оглавления таблиц: %s", exc)

    return ""


def ensure_valid_spreadsheet(gc: gspread.Client) -> str:
    current_id = get_current_data_sheet_id(gc)
    if not current_id:
        raise ExistingSpreadsheetRequiredError(
            "Активная Google-таблица не найдена в архивном индексе. "
            "Автосоздание отключено: укажите существующую активную таблицу вручную."
        )

    try:
        spreadsheet = gc.open_by_key(current_id)
    except Exception as exc:
        raise ExistingSpreadsheetRequiredError(
            f"Активная Google-таблица недоступна: {current_id}. "
            "Автосоздание отключено: проверьте доступ сервисного аккаунта и запись в индексе."
        ) from exc

    total_cells = sum(ws.row_count * ws.col_count for ws in spreadsheet.worksheets())
    if total_cells > MAX_SPREADSHEET_CELLS:
        raise ExistingSpreadsheetRequiredError(
            f"Активная Google-таблица {current_id} превысила безопасный лимит по размеру. "
            "Автосоздание отключено: создайте новую таблицу вручную и отметьте её как активную."
        )

    return current_id
