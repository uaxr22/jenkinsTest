

import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import logging

# Set up logging
logging.basicConfig(
    filename='data_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Connection strings
# Connection strings
on_prem_conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=FEMI-LOUNGE;"
    "Database=Learn_T_SQL;"
    #"UID=your_username;"
    #"PWD=your_password;"
    "Trusted_Connection=yes;"
)
# Define connection parameters
#server = 'FEMI-LOUNGE'       # Replace with your server name or IP address
#database = 'Learn_T_SQL'             # Database name
#driver = 'ODBC Driver 17 for SQL Server'  # Ensure this driver is installed

#conn = pyodbc.connect(
       # 'DRIVER={ODBC Driver 17 for SQL Server};'
      #  'SERVER=FEMI-LOUNGE;'
      #  'DATABASE=Learn_T_SQL;'
       # 'Trusted_Connection=yes;'
   # )

# Create a connection URL with Windows Authentication
#on_prem_conn_str = (
   #'mssql+pyodbc://@{server}/{database}?'
   # 'driver={driver}&'
   # 'Trusted_Connection=yes'
#).format(server=server, database=database, driver=driver)


azure_conn_url = URL.create(
    "mssql+pyodbc",
    username="dblearn",
    password="Olodumare65",
    host="dblearn.database.windows.net",
    database="dblearn",
    query={"driver": "ODBC Driver 17 for SQL Server"}
)


# Extract function
def extract_data():
    logging.info("Starting data extraction...")
    try:
        query = "SELECT * FROM Sales WHERE SalesDate = CAST(GETDATE() AS DATE)"
        with pyodbc.connect(on_prem_conn_str) as conn:
            data = pd.read_sql(query, conn)  # Fetch data into a DataFrame
        logging.info(f"Extracted {len(data)} rows from on-premises database.")
        return data
    except Exception as e:
        logging.error(f"Error during data extraction: {e}")
        raise


# Transform function
def transform_data(data):
    logging.info("Starting data transformation...")
    try:
        # Example transformation: Add a processed timestamp
        data['ProcessedTimestamp'] = pd.Timestamp.now()
        logging.info("Data transformation completed.")
        return data
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise


# Load function
def load_data(data):
    logging.info("Starting data loading...")
    try:
        engine = create_engine(azure_conn_url)
        with engine.connect() as conn:
            data.to_sql('Sales', conn, if_exists='append', index=False)  # Append data
        logging.info(f"Successfully loaded {len(data)} rows into Azure SQL Database.")
    except Exception as e:
        logging.error(f"Error during data loading: {e}")
        raise


# Pipeline orchestrator
def data_pipeline():
    try:
        logging.info("Data pipeline started.")
        
        # Extract
        data = extract_data()
        
        # Transform
        transformed_data = transform_data(data)
        
        # Load
        load_data(transformed_data)
        
        logging.info("Data pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Data pipeline failed: {e}")


# Main entry point
if __name__ == '__main__':
    data_pipeline()
