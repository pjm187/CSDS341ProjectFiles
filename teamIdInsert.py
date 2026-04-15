import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv() 

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_KEY not found in .env file!")
    exit(1)

supabase: Client = create_client(url, key)

team_directly = './teams'

all_team_data = []

for filename in os.listdir(team_directly):
    filename_lower = filename.lower()


    if filename_lower.startswith("formula1_") and "teams" in filename_lower:
        filepath = os.path.join(team_directly, filename)
        df = pd.read_csv(filepath)
        all_team_data.append(df)

if len(all_team_data) == 0:
    print(f"Error: No matching files found in '{team_directly}'.")
    exit()

combined_teams_df = pd.concat(all_team_data, ignore_index=True)

base_teams_df = combined_teams_df[['Base', 'First Team Entry']]
base_teams_df = base_teams_df.rename(columns ={'Base': 'base', 'First Team Entry': 'firstentry'})

unique_teams_df = base_teams_df.drop_duplicates()
unique_teams_df.insert(0, 'teamid', range(1, 1 + len(unique_teams_df)))
data_to_insert = unique_teams_df.to_dict(orient='records')

if len(data_to_insert) > 0:
    response = supabase.table("team").upsert(data_to_insert).execute()
    print(f"Successfully inserted {len(data_to_insert)} unique base teams!")
else:
    print("No team data found to insert.")
