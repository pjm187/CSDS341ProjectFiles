import os
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

# 3. Process the CSV Files
location_directory = './calendars'
unique_gp_names = set()

# Loop through the calendars directory to find the datasets
for filename in os.listdir(location_directory):
    filename_lower = filename.lower()

    # Match files that belong to the formula1 calendar datasets
    if filename_lower.startswith("formula1_") and "calendar" in filename_lower:
        filepath = os.path.join(location_directory, filename)
        df = pd.read_csv(filepath)
        df.columns = df.columns.str.strip()

        if 'GP Name' in df.columns:
            for gp in df['GP Name'].dropna().unique():
                unique_gp_names.add(str(gp).strip())
        else:
            print(f"found {filename}, but missing gp column")

# Safety check if directory is empty or missing
if len(unique_gp_names) == 0:
    print(f"Error: no file '{location_directory}")
    exit()

data_to_insert = []

for index, gp_name in enumerate(sorted(unique_gp_names), start=1):
    data_to_insert.append({
        'gpid' : index,
        'gpname' : gp_name
    })

# 6. Insert into Database
if len(data_to_insert) > 0:
    # Upsert the data into the 'circuit' table
    response = supabase.table("grandprix").upsert(data_to_insert).execute()    
    print(f"Successfully inserted {len(data_to_insert)} circuits!")
else:
    # Minor typo in your print statement here (says teamseason instead of circuit), but logic is correct!
    print("No Grand Prix data found")