# database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import MYSQL_URL, SQLITE_URL

def get_mysql_engine():
    return create_engine(MYSQL_URL, pool_pre_ping=True)

def get_sqlite_engine():
    return create_engine(SQLITE_URL, future=True)

def get_mysql_session():
    engine = get_mysql_engine()
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()

def get_sqlite_session():
    engine = get_sqlite_engine()
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()
