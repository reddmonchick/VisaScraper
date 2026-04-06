from __future__ import annotations

from dataclasses import dataclass

BATCH_APPLICATION_HEADERS = [
    "Batch No",
    "Register Number",
    "Full Name",
    "Date of Birth",
    "Visitor Visa Number",
    "Passport Number",
    "Payment Date",
    "Visa Type",
    "Status",
    "Action Link",
    "Account",
]
BATCH_MANAGER_HEADERS = [
    "Full Name",
    "Visa Type",
    "Payment Date",
    "Status",
    "Action Link",
    "Account",
]
STAY_PERMIT_HEADERS = [
    "Name",
    "Type of Stay Permit",
    "Visa Type",
    "Arrival Date",
    "Issue Date",
    "Expired Date",
    "Status",
    "Action Link",
    "Passport Number",
    "Account",
]

IDX_BA_ACCOUNT = 10
IDX_MGR_ACCOUNT = 5
IDX_MGR_PAYMENT_DATE = 2
IDX_SP_ACCOUNT = 9
PAYMENT_DATE_FORMAT = "%d-%m-%Y"


@dataclass(slots=True)
class BatchApplicationData:
    batch_no: str
    register_number: str
    full_name: str
    visitor_visa_number: str
    passport_number: str
    payment_date: str
    visa_type: str
    status: str
    action_link: str
    account: str
    birth_date: str

    def to_db_dict(self) -> dict[str, str]:
        return {
            "batch_no": self.batch_no,
            "register_number": self.register_number,
            "full_name": self.full_name,
            "visitor_visa_number": self.visitor_visa_number,
            "passport_number": self.passport_number,
            "payment_date": self.payment_date,
            "visa_type": self.visa_type,
            "status": self.status,
            "action_link": self.action_link,
            "account": self.account,
            "birth_date": self.birth_date,
        }

    def to_client_table_row(self) -> list[str]:
        return [
            self.batch_no,
            self.register_number,
            self.full_name,
            self.birth_date,
            self.visitor_visa_number,
            self.passport_number,
            self.payment_date,
            self.visa_type,
            self.status,
            self.action_link,
            self.account,
        ]

    def to_manager_row(self) -> list[str]:
        return [
            self.full_name,
            self.visa_type,
            self.payment_date,
            self.status,
            self.action_link,
            self.account,
        ]


@dataclass(slots=True)
class StayPermitData:
    reg_number: str
    name: str
    type_of_staypermit: str
    visa_type: str
    passport_number: str
    arrival_date: str
    issue_date: str
    expired_date: str
    status: str
    action_link: str
    account: str

    def to_db_dict(self) -> dict[str, str]:
        return {
            "reg_number": self.reg_number,
            "name": self.name,
            "type_of_staypermit": self.type_of_staypermit,
            "visa_type": self.visa_type,
            "passport_number": self.passport_number,
            "arrival_date": self.arrival_date,
            "issue_date": self.issue_date,
            "expired_date": self.expired_date,
            "status": self.status,
            "action_link": self.action_link,
            "account": self.account,
        }

    def to_sheet_row(self) -> list[str]:
        return [
            self.name,
            self.type_of_staypermit,
            self.visa_type,
            self.arrival_date,
            self.issue_date,
            self.expired_date,
            self.status,
            self.action_link,
            self.passport_number,
            self.account,
        ]
