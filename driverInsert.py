import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. Load Credentials from .env
load_dotenv() 

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

# Safety check to ensure credentials were found before proceeding
if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_KEY not found in .env file!")
    exit(1)

# Initialize the Supabase client
supabase: Client = create_client(url, key)

# Define the folder path where the F1 driver/season CSV files are stored
driver_directly = './drivers'

# List to hold the individual DataFrames created from each CSV
all_driver_data = []

# 2. Process the CSV Files
for filename in os.listdir(driver_directly):
    filename_lower = filename.lower()

    # Match files that belong to the formula1 season datasets
    if filename_lower.startswith("formula1_") and "season" in filename_lower:
        filepath = os.path.join(driver_directly, filename)
        
        # Read the CSV file into a Pandas DataFrame and append it
        df = pd.read_csv(filepath)
        all_driver_data.append(df)

# Safety check: exit if no matching files were found
if len(all_driver_data) == 0:
    print(f"Error: No matching files found in '{driver_directly}'.")
    exit()

# Combine all individual yearly DataFrames into one large DataFrame
combined_drivers_df = pd.concat(all_driver_data, ignore_index=True)

# 3. Standardize Columns
# Define exactly which columns we need to extract for the database
required_columns = [
    'Driver',
    'Abbreviation',
    'Country',
    'Date of Birth',
    'Place of Birth'
]

# Ensure every required column exists. If a CSV from an older year didn't 
# have one of these columns, this safely creates it and fills it with empty data (pd.NA)
for col in required_columns:
    if col not in combined_drivers_df.columns:
        combined_drivers_df[col] = pd.NA

# Filter the DataFrame down to just the columns we care about
driver_df = combined_drivers_df[required_columns]

# Rename columns to perfectly match the PostgreSQL schema (lowercase, no spaces)
driver_df = driver_df.rename(columns ={
    'Driver' : "name",
    'Abbreviation' : "abbreviation",
    'Country' : "nationality",
    'Date of Birth' : 'dateofbirth',
    'Place of Birth' : 'placeofbirth'
})

# 4. Data Cleaning & Formatting
# Convert dates to PostgreSQL's required YYYY-MM-DD format. 
# 'dayfirst=True' helps Pandas correctly parse European date formats (DD/MM/YYYY)
driver_df['dateofbirth'] = pd.to_datetime(driver_df['dateofbirth'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')

# Helper function: F1 drivers usually have a 3-letter abbreviation (e.g., HAM for Hamilton).
# If a dataset is missing this, grab the last name and use its first 3 letters.
def fill_abbreviation(row):
    if pd.isna(row['abbreviation']):
        # Split the full name by spaces, take the last chunk (last name)
        last_name = str(row['name']).split()[-1]
        return last_name[:3].upper() # Return first 3 letters capitalized
    return row['abbreviation']

# Apply the helper function across every row
driver_df['abbreviation'] = driver_df.apply(fill_abbreviation, axis = 1)

# Drop duplicate rows based on the driver's name, so each driver is only inserted once
unique_drivers_df = driver_df.drop_duplicates(subset= ['name'])

# 5. Prepare for Insertion
# Generate a primary key 'driverid' starting from 1
unique_drivers_df.insert(0, 'driverid', range(1, 1 + len(unique_drivers_df)))

# Convert Pandas NaN values into Python None objects. 
# (Supabase/Postgres requires actual nulls, and will crash if given Pandas NaNs)
unique_drivers_df = unique_drivers_df.astype(object).where(pd.notna(unique_drivers_df), None)

# Convert the cleaned DataFrame into a list of dictionaries
data_to_insert = unique_drivers_df.to_dict(orient='records')

# 6. Insert into Database
if len(data_to_insert) > 0:
    # Upsert the data into the 'driver' table
    response = supabase.table("driver").upsert(data_to_insert).execute()
    print(f"Successfully inserted {len(data_to_insert)} unique drivers")
else:
    print("No driver data found to insert.") # Adjusted typo from "team data"