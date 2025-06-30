import gspread

def setup_google_sheet(credentials_path: dict, sheet_id: str) -> tuple:
    gc = gspread.service_account_from_dict(credentials_path)
    spreadsheet = gc.open_by_key(sheet_id)
    return gc, spreadsheet

def prepare_worksheet(spreadsheet, worksheet_name: str):
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(worksheet_name, rows=1000, cols=20)
    return worksheet