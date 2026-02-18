Setup Instructions
Install dependencies
Run:
pip install -r requirements.txt

Configure environment variables
You must set both the MySQL (Sakila) connection and the SQLite database location.

On macOS or Linux:
export MYSQL_URL="mysql+pymysql://username:password@localhost:3306/sakila"
export SQLITE_URL="sqlite:///analytics_sakila.db"

On Windows (PowerShell):
setx MYSQL_URL "mysql+pymysql://username:password@localhost:3306/sakila"
setx SQLITE_URL "sqlite:///analytics.db"