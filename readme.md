# Sakila Sync

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
