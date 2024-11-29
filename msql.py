import pandas as pd
from sqlalchemy import create_engine

# Establish database connection (update connection details as needed)
engine = create_engine('mssql+pyodbc://username:password@hostname:port/Learnsql?driver=ODBC+Driver+17+for+SQL+Server')

# Execute validation queries
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
    result = pd.read_sql(query, engine)
    return result[result['validation_status'] == 'Invalid']

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
    result = pd.read_sql(query, engine)
    return result[result['validation_status'] == 'Invalid']

def check_invalid_orders():
    query = """
    SELECT 
        order_id,
        status,
        order_total
    FROM Orders
    WHERE order_total < 0 OR status NOT IN ('Shipped', 'Pending', 'Cancelled');
    """
    result = pd.read_sql(query, engine)
    return result

# Run validations
invalid_status = validate_order_status()
invalid_totals = validate_order_total()
invalid_orders = check_invalid_orders()

# Report results
if not invalid_status.empty:
    print("Invalid Order Status Found:\n", invalid_status)
if not invalid_totals.empty:
    print("Invalid Order Totals Found:\n", invalid_totals)
if not invalid_orders.empty:
    print("Invalid Orders Found:\n", invalid_orders)
