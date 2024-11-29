
import pyodbc
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="data_pipeline.log",
    filemode="a"
)

def load_data_to_target(data, connection):
    """
    Load data into the target database.
    """
    cursor = connection.cursor()
    try:
        logging.info("Loading data into the target database.")

        # Insert query excludes SaleID since it's an identity column
        insert_query = """
        INSERT INTO Sales (ProductName, Quantity, Price, Total, SalesDate, Region, ProcessedTimestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(insert_query, [
            (row["ProductName"], row["Quantity"], row["Price"], row["Total"], row["SalesDate"], row["Region"], row["ProcessedTimestamp"])
            for row in data
        ])
        connection.commit()
        logging.info("Data successfully loaded into the target database.")
    except Exception as e:
        logging.error(f"Error during data loading: {e}")
        raise
    finally:
        cursor.close()

def fetch_data_from_source(connection):
    """
    Fetch data from the source database.
    """
    cursor = connection.cursor()
    try:
        logging.info("Fetching data from the source database.")
        query = """
        SELECT ProductName, Quantity, Price, (Quantity * Price) AS Total, SalesDate, Region
        FROM Sales
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Transform data into a list of dictionaries
        data = []
        for row in rows:
            data.append({
                "ProductName": row.ProductName,
                "Quantity": row.Quantity,
                "Price": row.Price,
                "Total": row.Total,
                "SalesDate": row.SalesDate,
                "Region": row.Region,
                "ProcessedTimestamp": datetime.now(),
            })

        logging.info("Data fetched successfully.")
        return data
    except Exception as e:
        logging.error(f"Error during data fetch: {e}")
        raise
    finally:
        cursor.close()

def main():
    # Connection strings
    source_conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=FEMI-LOUNGE;"
        "Database=Learn_T_SQL;"
        "Trusted_Connection=yes;"
    )
    target_conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=dblearn.database.windows.net;"
        "Database=dblearn;"
        "UID=dblearn;"
        "PWD=Olodumare65;"
    )

    try:
        # Connect to source and target databases
        source_conn = pyodbc.connect(source_conn_str)
        target_conn = pyodbc.connect(target_conn_str)

        # Fetch data from source
        data = fetch_data_from_source(source_conn)

        # Load data into target
        load_data_to_target(data, target_conn)

        logging.info("Data pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Data pipeline failed: {e}")
    finally:
        # Close connections
        if 'source_conn' in locals():
            source_conn.close()
        if 'target_conn' in locals():
            target_conn.close()

if __name__ == "__main__":
    main()
