
import csv

# Path to your CSV file
csv_file_path = 'sales_data.csv'

# Sample data for the CSV file
data = [
    ["SaleID", "ProductName", "Quantity", "Price", "SalesDate"],
    [1, "Product A", 10, 15.5, "2024-11-20"],
    [2, "Product B", 5, 25.0, "2024-11-20"],
    [3, "Product C", 12, 7.75, "2024-11-20"],
    [4, "Product D", 7, 20.0, "2024-11-20"],
    [5, "Product E", 8, 12.3, "2024-11-20"]
]

# Writing data to the CSV file
with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

print(f"CSV file '{csv_file_path}' created successfully!")
