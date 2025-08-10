from dataclasses import dataclass, asdict

@dataclass
class BatchApplicationData:
    """Класс для представления распарсенных данных Batch Application."""
    batch_no: str
    register_number: str
    full_name: str
    visitor_visa_number: str
    passport_number: str
    payment_date: str
    visa_type: str
    status: str
    action_link: str # Это будет публичная ссылка с Яндекс.Диска
    account: str
    birth_date: str

    def to_client_table_row(self) -> list:
        """Преобразует объект в список для записи в Google Sheets (Batch Application)."""
        return [
            self.batch_no, self.register_number, self.full_name, self.birth_date,
            self.visitor_visa_number, self.passport_number, self.payment_date,
            self.visa_type, self.status, self.action_link, self.account
        ]

    def to_manager_row(self) -> list:
        """Преобразует объект в список для записи в Google Sheets (Batch Application Manager)."""
        return [
            self.full_name, self.visa_type, self.payment_date,
            self.status, self.action_link, self.account
        ]

    def to_db_dict(self) -> dict:
        """Преобразует объект в словарь для сохранения в БД."""
        return asdict(self)

@dataclass
class StayPermitData:
    """Класс для представления распарсенных данных Stay Permit."""
    reg_number: str
    name: str
    type_of_staypermit: str
    visa_type: str
    passport_number: str
    arrival_date: str
    issue_date: str
    expired_date: str
    status: str
    action_link: str # Это будет публичная ссылка с Яндекс.Диска
    account: str

    def to_sheet_row(self) -> list:
        """Преобразует объект в список для записи в Google Sheets (Stay Permit)."""
        return [
            self.name, self.type_of_staypermit, self.visa_type,
            self.arrival_date, self.issue_date, self.expired_date,
            self.status, self.action_link, self.passport_number, self.account
        ]

    def to_db_dict(self) -> dict:
        """Преобразует объект в словарь для сохранения в БД."""
        return asdict(self)
