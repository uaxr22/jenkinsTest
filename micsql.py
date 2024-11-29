import pandas as pd
from sqlalchemy import create_engine
import urllib

# Define connection parameters
server = 'FEMI-LOUNGE'       # Replace with your server name or IP address
database = 'Learn_T_SQL'             # Database name
driver = 'ODBC Driver 17 for SQL Server'  # Ensure this driver is installed

# Create a connection URL with Windows Authentication
connection_url = (
    'mssql+pyodbc://@{server}/{database}?'
    'driver={driver}&'
    'Trusted_Connection=yes'
).format(server=server, database=database, driver=driver)

# Create SQLAlchemy engine
engine = create_engine(connection_url)

# Define validation functions
def validate_order_status():
    query = """
    SELECT 
        order_id, 
        status AS actual_status,
        CASE 
            WHEN status NOT IN ('Shipped', 'Pending', 'Cancelled') THEN 'Invalid'
            ELSE 'Valid'
        END AS validation_status
    FROM Orders;
    """
    try:
        result = pd.read_sql(query, engine)
        invalid_records = result[result['validation_status'] == 'Invalid']
        return invalid_records
    except Exception as e:
        print(f"Error in validate_order_status: {e}")
        return pd.DataFrame()

def validate_order_total():
    query = """
    SELECT 
        o.order_id,
        o.order_total AS reported_total,
        SUM(oi.quantity * oi.price) AS calculated_total,
        CASE 
            WHEN o.order_total != SUM(oi.quantity * oi.price) THEN 'Invalid'
            ELSE 'Valid'
        END AS validation_status
    FROM Orders o
    JOIN OrderItems oi ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.order_total;
    """
    try:
        result = pd.read_sql(query, engine)
        invalid_records = result[result['validation_status'] == 'Invalid']
        return invalid_records
    except Exception as e:
        print(f"Error in validate_order_total: {e}")
        return pd.DataFrame()

def check_invalid_orders():
    query = """
    SELECT 
        order_id,
        status,
        order_total
    FROM Orders
    WHERE order_total < 0 OR status NOT IN ('Shipped', 'Pending', 'Cancelled');
    """
    try:
        result = pd.read_sql(query, engine)
        return result
    except Exception as e:
        print(f"Error in check_invalid_orders: {e}")
        return pd.DataFrame()

# Run validations
invalid_status = validate_order_status()
invalid_totals = validate_order_total()
invalid_orders = check_invalid_orders()

# Report results
if not invalid_status.empty:
    print("Invalid Order Status Found:\n", invalid_status.to_string(index=False))
else:
    print("No Invalid Order Status Found.")

if not invalid_totals.empty:
    print("\nInvalid Order Totals Found:\n", invalid_totals.to_string(index=False))
else:
    print("\nNo Invalid Order Totals Found.")

if not invalid_orders.empty:
    print("\nInvalid Orders Found:\n", invalid_orders.to_string(index=False))
else:
    print("\nNo Invalid Orders Found.")
