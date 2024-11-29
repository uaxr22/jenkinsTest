
import sqlite3

# Path to your SQLite database file
db_path = 'sales.db'

# Connect to the SQLite database (it will be created if it doesn't exist)
conn = sqlite3.connect(db_path)

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Create the Sales table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Sales (
        SaleID INTEGER PRIMARY KEY AUTOINCREMENT,
        ProductName TEXT,
        Quantity INTEGER,
        Price REAL,
        SalesDate TEXT,
        Total REAL
    )
""")

# Commit the changes and close the connection
conn.commit()
conn.close()

print("SQLite database and table created successfully!")
