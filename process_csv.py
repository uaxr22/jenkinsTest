import os
import pandas as pd

def process_csv_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            data = pd.read_csv(file_path)
            # Perform some processing (e.g., calculate averages)
            data['average'] = data.mean(axis=1)
            data.to_csv(f"{directory}/processed_{filename}", index=False)
            print(f"Processed {filename}")

# Run the function with the path to the CSV directory
#process_csv_files(C:\Users\Oluwafemi\Documents\sample data_python)  
process_csv_files("../sample data_python")
# Adjust this if your directory structure is different