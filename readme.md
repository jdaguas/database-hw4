# ORM Data Sync Process

Syncs data from a MySQL Sakila database into a local SQLite analytics database using SQLAlchemy.

## Setup Instructions

### 1. Install Dependencies

pip install -r requirements.txt

### 2. Configure Environment Variables

You must set both:

- MYSQL_URL (Sakila MySQL connection)
- SQLITE_URL (Local SQLite database location)

macOS / Linux:

export MYSQL_URL="mysql+pymysql://username:password@localhost:3306/sakila"
export SQLITE_URL="sqlite:///analytics_sakila.db"

Windows (PowerShell):

setx MYSQL_URL "mysql+pymysql://username:password@localhost:3306/sakila"
setx SQLITE_URL "sqlite:///analytics_sakila.db"

After running setx, restart your terminal.

## Running the Application

python app.py init
python app.py sync
python app.py validate
python app.py incremental

## Running Tests

pytest

## Sources

https://stackoverflow.com/questions/16981921/testing-command-line-programs-with-python
https://stackoverflow.com/questions/18741784/testing-sqlalchemy-with-sqlite-in-memory
https://docs.pytest.org/en/stable/how-to/tmp_path.html
https://stackoverflow.com/questions/2894217/using-sqlite3-in-memory-database-for-unit-testing
https://stackoverflow.com/questions/26298611/pytest-and-sqlalchemy-rollback-between-tests