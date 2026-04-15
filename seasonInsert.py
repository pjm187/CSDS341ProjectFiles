import os
import argparse  # Note: Imported but not used in this script
import pandas as pd  # Note: Imported but not actually needed since we don't use pd.read_csv here!
from dotenv import load_dotenv
from supabase import create_client, Client

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

# Define the folder path where the F1 season CSV files are stored
database_directory = './drivers'

# Use a Python 'set' instead of a list. Sets automatically prevent duplicate values, 
# ensuring we only get one entry per year even if multiple files mention the same year.
unique_years = set()

# 2. Extract Data from Filenames
for filename in os.listdir(database_directory):
    filename_lower = filename.lower()

    # Match files that belong to the formula1 season datasets
    if filename_lower.startswith("formula1_") and "season" in filename_lower:
        # Extract the year from the filename string. 
        # Example: "formula1_2023_season.csv" -> splits at '_' -> takes the 2nd chunk "2023"
        year_str = filename.split('_')[1][:4]
        
        # Add the extracted year to our set as an integer
        unique_years.add(int(year_str))

# 3. Format Data for Insertion
# Convert our set of unique years into a sorted list of dictionaries.
# This format [{"seasonyear": 2020}, {"seasonyear": 2021}] is exactly what Supabase expects.
data_to_insert = [{"seasonyear": year} for year in sorted(unique_years)]

# 4. Insert into Database
if len(data_to_insert) == 0:
    print(f"Error: no matching files found") # fixed slight typo in print statement here
else:
    # Upsert the data into the 'season' table
    response = supabase.table("season").upsert(data_to_insert).execute()
    print(f"Successfully inserted {len(data_to_insert)} Seasons")