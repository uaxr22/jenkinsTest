import pandas as pd
import sqlite3
import logging
from datetime import datetime

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,  # Can be changed to DEBUG or ERROR based on requirements
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler()
    ]
)

def extract_data(file_path):
    """
    Extract data from a CSV file.
    """
    try:
        logging.info("Starting data extraction from the CSV file.")
        df = pd.read_csv(file_path)
        logging.info(f"Extracted {len(df)} rows of data.")
        return df
    except Exception as e:
        logging.error(f"Error during data extraction: {e}")
        raise  # Reraise exception to be handled later

def transform_data(df):
    """
    Transform the data (e.g., cleaning, adding new columns).
    """
    try:
        logging.info("Starting data transformation.")
        
        # Convert 'SalesDate' to datetime (if possible, else set to NaT)
        df['SalesDate'] = pd.to_datetime(df['SalesDate'], errors='coerce')
        
        # Calculate 'Total' as Quantity * Price
        df['Total'] = df['Quantity'] * df['Price']
        
        # Drop rows with missing values in critical columns (like 'SalesDate', 'Quantity', 'Price')
        df = df.dropna(subset=['SalesDate', 'Quantity', 'Price'])
        
        logging.info(f"Data transformation complete. {len(df)} rows after transformation.")
        return df
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise  # Reraise exception to be handled later

def load_data_to_sqlite(df, db_path):
    """
    Load data into SQLite database.
    """
    try:
        logging.info("Starting data load into SQLite database.")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create table if it doesn't exist (ensure 'SalesDate' is stored as TEXT in SQLite)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Sales (
                SaleID INTEGER PRIMARY KEY AUTOINCREMENT,  -- Auto-increment SaleID
                ProductName TEXT,
                Quantity INTEGER,
                Price REAL,
                SalesDate TEXT,
                Total REAL
            )
        """)

        # Insert data into Sales table (SaleID will be auto-incremented, so we omit it)
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO Sales (ProductName, Quantity, Price, SalesDate, Total)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row['ProductName'], 
                row['Quantity'], 
                row['Price'], 
                row['SalesDate'].strftime('%Y-%m-%d %H:%M:%S'),  # Ensure date is in the correct format
                row['Total']
            ))

        # Commit and close
        conn.commit()
        conn.close()
        logging.info(f"Data load complete. {len(df)} rows inserted.")
    except Exception as e:
        logging.error(f"Error during data load: {e}")
        raise  # Reraise exception to be handled later

def main():
    file_path = 'sales_data.csv'  # Path to the source CSV file
    db_path = 'sales.db'          # Path to the target SQLite database

    try:
        # Extract data
        df = extract_data(file_path)

        # Transform data
        df = transform_data(df)

        # Load data to SQLite database
        load_data_to_sqlite(df, db_path)

        logging.info("ETL pipeline completed successfully.")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
    finally:
        logging.info("ETL pipeline execution finished.")

if __name__ == "__main__":
    main()
