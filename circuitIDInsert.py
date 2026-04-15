import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from a .env file (keeps API keys secure)
load_dotenv() 

# Retrieve the Supabase URL and Key from the environment
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

# Safety check to ensure credentials were found before proceeding
if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_KEY not found in .env file!")
    exit(1)

# Initialize the Supabase client to interact with the database
supabase: Client = create_client(url, key)

# Define the folder path where the F1 calendar CSV files are stored
circuit_directly = './calendars'

# List to hold the individual DataFrames created from each CSV
all_circuit_data = []

# Loop through every file in the calendars directory
for filename in os.listdir(circuit_directly):
    filename_lower = filename.lower()

    # Check if the file matches the expected naming convention for calendar datasets
    if filename_lower.startswith("formula1_") and "calendar" in filename_lower:
        filepath = os.path.join(circuit_directly, filename)
        
        # Read the CSV file into a Pandas DataFrame and add it to our list
        df = pd.read_csv(filepath)
        all_circuit_data.append(df)

# Safety check: exit if no matching files were found
if len(all_circuit_data) == 0:
    print(f"Error: No matching files found in '{circuit_directly}'.")
    exit()

# Combine all individual yearly calendar DataFrames into one massive DataFrame
combined_circuits_df = pd.concat(all_circuit_data, ignore_index=True)

# Extract only the 'City' and 'Country' columns, as this script is just for locations
base_circuit_df = combined_circuits_df[['City', 'Country']]

# Rename columns to match the PostgreSQL database schema (lowercase)
base_circuit_df = base_circuit_df.rename(columns={'City': 'city', 'Country': 'country'})

# Drop duplicate rows so each city/country pair only appears exactly once
unique_circuit_df = base_circuit_df.drop_duplicates()

# Generate a unique primary key ('locationid') starting from 1
unique_circuit_df.insert(0, 'locationid', range(1, 1 + len(unique_circuit_df)))

# Convert the Pandas DataFrame into a list of dictionaries (the format Supabase needs)
data_to_insert = unique_circuit_df.to_dict(orient='records')

# If we have data, push it to the 'circuitlocation' table in Supabase via an UPSERT
if len(data_to_insert) > 0:
    response = supabase.table("circuitlocation").upsert(data_to_insert).execute()
    print(f"Successfully inserted {len(data_to_insert)} unique location!")
else:
    print("No location data found to insert.")