
import sqlite3

def query_sales():
    # Connect to SQLite database
    conn = sqlite3.connect('sales.db')
    cursor = conn.cursor()

    try:
        # Select all records
        cursor.execute("SELECT * FROM Sales")
        rows = cursor.fetchall()

        print("All Sales Records:")
        for row in rows:
            print(row)

        # Select records with condition
        cursor.execute("SELECT ProductName, Quantity, Total FROM Sales WHERE Region = 'North'")
        rows = cursor.fetchall()

        print("\nSales in the North Region:")
        for row in rows:
            print(row)

        # Inserting new record
        cursor.execute("""
            INSERT INTO Sales (ProductName, Quantity, Price, SalesDate, Total)
            VALUES (?, ?, ?, ?, ?)
        """, ('Product Z', 15, 30.99, '2024-11-21 12:00:00', 464.85))

        conn.commit()
        print("\nNew record inserted.")

        # Updating a record
        cursor.execute("""
            UPDATE Sales
            SET Price = ?
            WHERE ProductName = ?
        """, (35.99, 'Product Z'))

        conn.commit()
        print("\nRecord updated.")

        # Delete a record
        cursor.execute("""
            DELETE FROM Sales WHERE ProductName = ?
        """, ('Product Z',))

        conn.commit()
        print("\nRecord deleted.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()

# Run the function
query_sales()
