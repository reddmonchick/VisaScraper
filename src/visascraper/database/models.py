from __future__ import annotations

from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True, index=True, nullable=False)
    password = Column(String, nullable=True)
    is_authorized = Column(Boolean, default=False, nullable=False)


class BatchApplication(Base):
    __tablename__ = "batch_applications"

    id = Column(Integer, primary_key=True)
    batch_no = Column(String)
    register_number = Column(String, index=True)
    full_name = Column(String, index=True)
    visitor_visa_number = Column(String)
    passport_number = Column(String, index=True)
    payment_date = Column(String)
    visa_type = Column(String)
    status = Column(String)
    action_link = Column(String)
    account = Column(String, index=True)
    birth_date = Column(String)
    last_status = Column(String, default=None)
    notified_as_new = Column(Boolean, default=False, nullable=False)


class StayPermit(Base):
    __tablename__ = "stay_permits"

    id = Column(Integer, primary_key=True)
    reg_number = Column(String, unique=True, nullable=False, index=True)
    name = Column(String)
    type_of_staypermit = Column(String)
    visa_type = Column(String)
    passport_number = Column(String, index=True)
    arrival_date = Column(String)
    issue_date = Column(String)
    expired_date = Column(String)
    status = Column(String)
    last_status = Column(String)
    action_link = Column(String)
    account = Column(String, index=True)
    notified_as_new = Column(Boolean, default=False, nullable=False)
