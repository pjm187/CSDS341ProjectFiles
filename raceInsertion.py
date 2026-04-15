import os
import argparse
import re
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
import datetime

# 1. Load Credentials from .env
load_dotenv() 

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

# Safety check for database credentials
if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_KEY not found in .env file!")
    exit(1)

# Initialize the Supabase Client
supabase: Client = create_client(url, key)

# 2. Fetch Foreign Key Data
# Query the database to get all existing circuits so we can link races to their physical tracks
dp_response = supabase.table('circuit').select('circuitid, circuitname').execute()
circuit_data = dp_response.data

# Create a lookup dictionary mapping the Circuit Name to its Database ID
# Example: {'Albert Park Circuit': 1}
circuitName_to_circuitid = {row['circuitname']: row['circuitid'] for row in circuit_data}

gp_response = supabase.table('grandprix').select('gpid, gpname').execute()
gp_data = gp_response.data
gpName_to_gpid = {row['gpname']: row['gpid'] for row in gp_data}


# Define where the calendar CSVs are stored
location_directory = './calendars'
all_race_data = []

# 3. Process the CSV Files
for filename in os.listdir(location_directory):
    filename_lower = filename.lower()

    # Match files that belong to the formula1 calendar datasets
    if filename_lower.startswith("formula1_") and "calendar" in filename_lower:
        filepath = os.path.join(location_directory, filename)
        df = pd.read_csv(filepath)
        
        # Use Regular Expressions (re) to find the 4-digit year inside the filename
        year_match = re.search(r'\d{4}', filename)
        if year_match:
            # Create a new column 'seasonyear' and assign the extracted year to every row in this dataset
            df['seasonyear'] = int(year_match.group())
        else:
            # Safely skip the file if it doesn't contain a year instead of crashing
            print(f"Skipping {filename}: Could not find a 4-digit year in the name.")
            continue

        all_race_data.append(df)

# Safety check if directory is empty or mapping failed
if len(all_race_data) == 0:
    print(f"Error: no file '{location_directory}'")
    exit()

# Combine all calendar DataFrames into one master DataFrame
combined_df = pd.concat(all_race_data, ignore_index=True)

# 4. Map the Foreign Keys
# Clean up the track name strings (strip whitespace, apply Title Case) 
# and use our dictionary to replace the string with the proper 'circuitid' integer
combined_df['circuitid'] = combined_df['Circuit Name'].str.strip().str.title().map(circuitName_to_circuitid)
combined_df['gpid'] = combined_df['GP Name'].str.strip().map(gpName_to_gpid)

# 5. Clean and Format the Data for Insertion
# Extract only the columns relevant to the Race table
race_df = combined_df[[
    'seasonyear',  'Round', 'gpid', 'Number of Laps', 'Race Date', 'circuitid'
    ]].copy()

# Rename columns to perfectly match the PostgreSQL schema (lowercase, no spaces)
race_df = race_df.rename(columns ={
        'Round': 'round', 
        'Number of Laps':'laps', 
        'Race Date':'racedate'
})

# Generate a primary key 'raceid' starting from 1
race_df.insert(0, 'raceid', range(1, 1 +  len(race_df)))

# Format the Race Date. 
# format='mixed' allows Pandas to handle datasets that switch between 'YYYY-MM-DD' and 'DD/MM/YYYY'
# dayfirst=True ensures things like 10/11/2020 are read as Nov 10th instead of Oct 11th.
race_df['racedate'] = pd.to_datetime(race_df['racedate'], format='mixed', dayfirst=True).dt.strftime('%Y-%m-%d')

# Convert Pandas NaN values into Python None objects for PostgreSQL compatibility
race_df = race_df.astype(object).where(pd.notna(race_df), None)
data_to_insert = race_df.to_dict(orient='records')

int_columns = ['raceid', 'seasonyear', 'round', 'gpid', 'laps', 'circuitid']
for record in data_to_insert:
    for col in int_columns:
        if record.get(col) is not None:
            record[col] = int(record[col]) 

# 6. Insert into Database
if len(data_to_insert) > 0:
    response = supabase.table("race").upsert(data_to_insert).execute()    
    print(f"Successfully inserted {len(data_to_insert)} races")
else:
    print("No racedata data found")