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

# Define the folder path where the F1 team CSV files are stored
team_directly = './teams'

# List to hold the individual DataFrames created from each CSV
all_team_data = []

# 2. Extract Data from Files
for filename in os.listdir(team_directly):
    filename_lower = filename.lower()

    # Match files that belong to the formula1 teams datasets
    if filename_lower.startswith("formula1_") and "teams" in filename_lower:
        filepath = os.path.join(team_directly, filename)
        
        # Read the CSV file into a Pandas DataFrame and append it to our list
        df = pd.read_csv(filepath)
        all_team_data.append(df)

# Safety check: exit if no matching files were found
if len(all_team_data) == 0:
    print(f"Error: No matching files found in '{team_directly}'.")
    exit()

# Combine all individual yearly team DataFrames into one massive DataFrame
combined_teams_df = pd.concat(all_team_data, ignore_index=True)

# 3. Clean and Format Data
# Extract only the physical 'Base' location and 'First Team Entry' year 
# (These are the attributes that generally don't change year-to-year)
base_teams_df = combined_teams_df[['Base', 'First Team Entry']]

# Rename columns to strictly match the PostgreSQL database schema (lowercase)
base_teams_df = base_teams_df.rename(columns={'Base': 'base', 'First Team Entry': 'firstentry'})

# Drop duplicate rows so each base/team entity only appears exactly once in the database
unique_teams_df = base_teams_df.drop_duplicates()

# Generate a unique primary key ('teamid') starting from 1
unique_teams_df.insert(0, 'teamid', range(1, 1 + len(unique_teams_df)))

# Convert the Pandas DataFrame into a list of dictionaries for Supabase
data_to_insert = unique_teams_df.to_dict(orient='records')

# 4. Insert into Database
if len(data_to_insert) > 0:
    # Upsert the data into the 'team' table
    response = supabase.table("team").upsert(data_to_insert).execute()
    print(f"Successfully inserted {len(data_to_insert)} unique base teams!")
else:
    print("No team data found to insert.")