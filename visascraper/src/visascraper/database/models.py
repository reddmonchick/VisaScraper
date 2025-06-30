from sqlalchemy import Column, Integer, String, Date, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True, index=True)
    password = Column(String)

class BatchApplication(Base):
    __tablename__ = 'batch_applications'

    id = Column(Integer, primary_key=True)
    batch_no = Column(String)
    register_number = Column(String)
    full_name = Column(String)
    visitor_visa_number = Column(String)
    passport_number = Column(String)
    payment_date = Column(String)
    visa_type = Column(String)
    status = Column(String)
    action_link = Column(String)
    account = Column(String)
    birth_date = Column(String)
    last_status = Column(String, default=None)

class StayPermit(Base):
    __tablename__ = 'stay_permits'

    id = Column(Integer, primary_key=True)
    reg_number = Column(String)
    name = Column(String)
    type_of_staypermit = Column(String)
    visa_type = Column(String)
    passport_number = Column(String)
    arrival_date = Column(String)
    issue_date = Column(String)
    expired_date = Column(String)
    status = Column(String)
    action_link = Column(String)
    account = Column(String)