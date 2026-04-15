import os
import argparse # Note: Imported but not currently used in the script
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. Load Credentials from .env
load_dotenv() 

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

# Safety check to ensure credentials were found
if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_KEY not found in .env file!")
    exit(1)

# Initialize the Supabase client
supabase: Client = create_client(url, key)

# 2. Fetch Foreign Key Data
# Query the existing 'circuitlocation' table to get the IDs of the locations we just inserted
dp_response = supabase.table('circuitlocation').select('locationid, city').execute()
circuit_data = dp_response.data

# Create a lookup dictionary mapping the City name to its Database ID (e.g., {'Melbourne': 1})
loctaion_to_circuitid = {row['city']: row['locationid'] for row in circuit_data}

# 3. Process the CSV Files
location_directory = './calendars'
all_circuit_data = []

# Loop through the calendars directory to find the datasets
for filename in os.listdir(location_directory):
    filename_lower = filename.lower()

    # Match files that belong to the formula1 calendar datasets
    if filename_lower.startswith("formula1_") and "calendar" in filename_lower:
        filepath = os.path.join(location_directory, filename)
        df = pd.read_csv(filepath)
        all_circuit_data.append(df)

# Safety check if directory is empty or missing
if len(all_circuit_data) == 0:
    print(f"Error: no file '{location_directory}")
    exit()

# Combine all calendar DataFrames into one large DataFrame
combined_df = pd.concat(all_circuit_data, ignore_index=True)

# 4. Map the Foreign Keys
# Use the lookup dictionary we created earlier to replace the 'City' string with the proper 'locationid' integer
combined_df['locationid'] = combined_df['City'].map(loctaion_to_circuitid)

# 5. Clean and Format the Data for Insertion
# Extract only the columns relevant to the Circuit table (ignoring dates, rounds, etc.)
circuit_df = combined_df[[
    'locationid', 'Circuit Name', 'Circuit Length(km)', 'Turns', 'DRS Zones'
    ]].copy()

# Rename columns to perfectly match the PostgreSQL schema (all lowercase, no spaces/parentheses)
circuit_df = circuit_df.rename(columns ={
        'Circuit Name': 'circuitname', 
        'Circuit Length(km)':'circuitlength_km', 
        'Turns':'turns', 
        'DRS Zones':'drszones'
})

# Standardize the circuit names by removing accidental spaces and capitalizing the first letters
circuit_df['circuitname'] = circuit_df['circuitname'].str.strip()
circuit_df['circuitname'] = circuit_df['circuitname'].str.title()

# Drop duplicates so each physical race track is only inserted into the DB once
circuit_df = circuit_df.drop_duplicates(subset='circuitname')

# Generate a primary key 'circuitid' starting from 1
circuit_df.insert(0, 'circuitid', range(1, 1 +  len(circuit_df)))

# Convert the cleaned DataFrame into a list of dictionaries for Supabase
data_to_insert = circuit_df.to_dict(orient='records')

# 6. Insert into Database
if len(data_to_insert) > 0:
    # Upsert the data into the 'circuit' table
    response = supabase.table("circuit").upsert(data_to_insert).execute()    
    print(f"Successfully inserted {len(data_to_insert)} circuits!")
else:
    # Minor typo in your print statement here (says teamseason instead of circuit), but logic is correct!
    print("No circuit data found")