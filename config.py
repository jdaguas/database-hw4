import os

MYSQL_URL = os.getenv("MYSQL_URL")
SQLITE_URL = os.getenv("SQLITE_URL", "sqlite:///analytics_sakila.db")

if not MYSQL_URL:
    raise RuntimeError(
        "MYSQL_URL not set. Example:\n"
        "export MYSQL_URL='mysql+mysqlconnector://user:pass@localhost:3306/sakila'"
    )
