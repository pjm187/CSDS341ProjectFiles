import os
import pandas as pd
import re
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. Load Credentials from .env
load_dotenv() 

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

# Safety check to ensure credentials exist
if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_KEY not found in .env file!")
    exit(1)

# Initialize the Supabase client
supabase: Client = create_client(url, key)

# 2. Fetch Foreign Key Data
# Query the database to get all existing drivers and teams
driver_data = supabase.table("driver").select("driverid, name").execute().data
team_data = supabase.table("teamseason").select("teamid, shortname").execute().data

# Create lookup dictionaries mapping the names to their Database IDs
# Example: {'Lewis Hamilton': 1}
driver_map = {str(d['name']).strip(): d['driverid'] for d in driver_data}
# Example: {'Mercedes': 5}
team_map = {str(t['shortname']).strip(): t['teamid'] for t in team_data if t['shortname']}

# Define where the CSVs are stored
driver_directly = './drivers'
all_season_entries = []

# 3. Process the CSV Files
for filename in os.listdir(driver_directly):
    filename_lower = filename.lower()

    # Match files that belong to the formula1 season datasets
    if filename_lower.startswith("formula1_") and "season" in filename_lower:
        filepath = os.path.join(driver_directly, filename)
        df = pd.read_csv(filepath)

        # Standardize the column name for the driver's race number. 
        # Accounts for the dataset changing its headers over different years.
        df = df.rename(columns={
            'Race Number': 'Number',
            'No' : 'Number'
        })

        # Extract the 4-digit season year from the filename (e.g., '2023')
        year_match = re.search(r'\d{4}', filename)
        if not year_match:
            continue
        season_year = int(year_match.group())

        # Check for required columns and fill them with pandas NA if missing
        required_columns = ['Driver', 'Team' , 'Number']
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA

        # 4. Map the Data
        # Iterate over every row in the dataset to connect the driver to their team for this specific year
        for index, row in df.iterrows():
                driver_name = str(row['Driver']).strip() if pd.notna(row['Driver']) else ""
                team_name = str(row['Team']).strip() if pd.notna(row['Team']) else ""
                race_num = row['Number']

                # Look up the Foreign Keys using our dictionaries
                driver_id = driver_map.get(driver_name)
                team_id = team_map.get(team_name)

                # Warn us in the terminal if a team name from the CSV doesn't match the DB
                if not team_id:
                    print(f"Could not find team match for '{team_name}' in database")

                # If both foreign keys are successfully found, append the mapped record
                if driver_id and team_id:
                    all_season_entries.append({
                        'driverid': driver_id,
                        'seasonyear': season_year,
                        'teamid': team_id,
                        'racenumber': race_num
                    })

# Safety check if directory is empty or mapping failed completely
if len(all_season_entries) == 0:
    print(f"Error: No matching files found in '{driver_directly}'.")
    exit()

# 5. Prepare for Insertion
driver_season_df = pd.DataFrame(all_season_entries)

# Drop duplicates to ensure a driver isn't assigned to the same season twice
driver_season_df = driver_season_df.drop_duplicates(subset=['driverid', 'seasonyear'])

# Generate the primary key 'driverseasonid' starting from 1
driver_season_df.insert(0, 'driverseasonid', range(1, 1 + len(driver_season_df)))

# Convert Pandas NaN values into Python None objects for PostgreSQL compatibility
driver_season_df = driver_season_df.astype(object).where(pd.notna(driver_season_df), None)
data_to_insert = driver_season_df.to_dict(orient = 'records')

# Note: You have a redundant execute here right before the if statement. 
# You can actually delete this line below, since the if-statement handles it properly!
# response = supabase.table("driverseason").upsert(data_to_insert).execute()

# 6. Insert into Database
if len(data_to_insert) > 0:
    response = supabase.table("driverseason").upsert(data_to_insert).execute()
    print(f"Successfully inserted {len(data_to_insert)} unique driver-season records")
else:
    print("No team data found to insert.")