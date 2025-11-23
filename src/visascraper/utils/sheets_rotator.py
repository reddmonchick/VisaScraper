# utils/sheets_rotator.py
import os
from datetime import datetime
import gspread
from dotenv import load_dotenv

load_dotenv()

# ←←← ВСЁ ИЗ .env — красота!
ACCOUNTS_SHEET_ID = os.getenv("GOOGLE_ACCOUNTS_SHEET_ID")
ARCHIVE_INDEX_ID = os.getenv("GOOGLE_ARCHIVE_INDEX_ID")
TEMPLATE_SHEET_ID = os.getenv("GOOGLE_TEMPLATE_SHEET_ID", "")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
GOOGLE_CURRENT_SHEET_ID = os.getenv("GOOGLE_CURRENT_SHEET_ID")

def get_current_data_sheet_id(gc: gspread.Client) -> str:
    if not ARCHIVE_INDEX_ID:
        return ""
    try:
        index_ss = gc.open_by_key(ARCHIVE_INDEX_ID)
        ws = index_ss.sheet1
        values = ws.get_all_values()
        for row in reversed(values):
            if len(row) >= 4 and "активна" in row[3].lower():
                url = row[2]
                sheet_id = url.split("/d/")[-1].split("/")[0]
                print(f"Найдена активная таблица: {sheet_id}")
                return sheet_id
    except Exception as e:
        print(f"Ошибка чтения оглавления: {e}")
    return ""

def mark_as_active(gc: gspread.Client, sheet_id: str, title: str):
    if not ARCHIVE_INDEX_ID:
        return
    try:
        index_ss = gc.open_by_key(ARCHIVE_INDEX_ID)
        ws = index_ss.sheet1
        values = ws.get_all_values()
        for i, row in enumerate(values):
            if len(row) >= 4 and "активна" in row[3].lower():
                ws.update(f"D{i+1}", [["архив"]])
        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            title,
            f"https://docs.google.com/spreadsheets/d/{sheet_id}",
            "активна"
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        print(f"Таблица {title} помечена как активна")
    except Exception as e:
        print(f"Не удалось обновить оглавление: {e}")

def create_new_spreadsheet(gc: gspread.Client) -> str:
    month = datetime.now().strftime("%Y_%m")
    title = f"VisaData_{month}"
    print(f"Создаём новую таблицу: {title}")

    if not DRIVE_FOLDER_ID:
        raise Exception("Не указан GOOGLE_DRIVE_FOLDER_ID в .env!")

    # Создаём таблицу
    ss = gc.create(title, folder_id=DRIVE_FOLDER_ID)
    print(f"Таблица создана: {ss.id}")

    # === ЗАГОЛОВКИ ДЛЯ НАШИХ ЛИСТОВ ===
    headers = {
        "Batch Application": ['Batch No', 'Register Number', 'Full Name', 'Date of Birth', 'Visitor Visa Number',
                              'Passport Number', 'Payment Date', 'Visa Type', 'Status', 'Action Link', 'Account'],
        "Batch Application(Manager)": ['Full Name', 'Visa Type', 'Payment Date', 'Status', 'Action Link', 'Account'],
        "StayPermit": ['Name', 'Type of Stay Permit', 'Visa Type', 'Arrival Date', 'Issue Date',
                       'Expired Date', 'Status', 'Action Link', 'Passport Number', 'Account']
    }

    # 1. Переименовываем стандартный "Sheet1" в первый нужный лист
    sheet1 = ss.sheet1
    sheet1.update_title("Batch Application")
    sheet1.clear()  # очищаем
    sheet1.append_row(headers["Batch Application"], value_input_option='USER_ENTERED')
    print("Лист переименован и заполнен: Batch Application")

    # 2. Добавляем остальные два листа
    ss.add_worksheet(title="Batch Application(Manager)", rows=1000, cols=20)
    ws_mgr = ss.worksheet("Batch Application(Manager)")
    ws_mgr.append_row(headers["Batch Application(Manager)"], value_input_option='USER_ENTERED')
    print("Создан лист: Batch Application(Manager)")

    ss.add_worksheet(title="StayPermit", rows=1000, cols=20)
    ws_stay = ss.worksheet("StayPermit")
    ws_stay.append_row(headers["StayPermit"], value_input_option='USER_ENTERED')
    print("Создан лист: StayPermit")

    # 3. Если есть шаблон — копируем дополнительные листы (по желанию)
    if TEMPLATE_SHEET_ID:
        try:
            template = gc.open_by_key(TEMPLATE_SHEET_ID)
            for ws_t in template.worksheets():
                if ws_t.title not in ["Batch Application", "Batch Application(Manager)", "StayPermit"]:
                    new_ws = ss.add_worksheet(title=ws_t.title, rows=1000, cols=20)
                    header = ws_t.row_values(1)
                    if header:
                        new_ws.append_row(header, value_input_option='USER_ENTERED')
            print("Дополнительные листы скопированы из шаблона")
        except Exception as e:
            print(f"Не удалось скопировать из шаблона: {e}")

    # Обновляем оглавление
    mark_as_active(gc, ss.id, title)
    print(f"НОВАЯ ТАБЛИЦА ГОТОВА: https://docs.google.com/spreadsheets/d/{ss.id}")
    return ss.id

def ensure_valid_spreadsheet(gc: gspread.Client) -> str:
    current_id = get_current_data_sheet_id(gc)

    if not current_id:
        print("Активная таблица не найдена → создаём новую")
        return create_new_spreadsheet(gc)

    try:
        ss = gc.open_by_key(current_id)
        total_cells = sum(ws.row_count * ws.col_count for ws in ss.worksheets())
        print(f"Текущая таблица: ~{total_cells:,} ячеек")

        if total_cells > 8_500_000:
            print("ЛИМИТ ПРЕВЫШЕН → создаём новую")
            return create_new_spreadsheet(gc)

        return current_id
    except Exception as e:
        print(f"Таблица недоступна ({current_id}): {e} → создаём новую")
        return create_new_spreadsheet(gc)