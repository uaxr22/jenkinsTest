import pyodbc

# Test connection
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=FEMI-LOUNGE;'
        'DATABASE=Learn_T_SQL;'
        'Trusted_Connection=yes;'
    )
    print("Connection successful!")
    conn.close()
except Exception as e:
    print("Error connecting to SQL Server:", e)
