import os
import re
import pandas as pd
import difflib
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

# 2. Fetch the required foreign key data
# We need to map strings from the CSVs to their actual Database Integer IDs
driver_data = supabase.table("driver").select("driverid, name").execute().data
team_data = supabase.table("teamseason").select("teamid, shortname").execute().data
gp_data = supabase.table("grandprix").select("gpid, gpname").execute().data
gpid_to_name = {row['gpid']: str(row['gpname']).strip().lower() for row in gp_data}
race_data = supabase.table("race").select("raceid, seasonyear, gpid").execute().data

# Create mapping dictionaries for quick lookups
driver_map = {str(d['name']).strip().lower(): d['driverid'] for d in driver_data}
db_driver_names = list(driver_map.keys())

# --- ROBUST RACE & TEAM ALIASES ---

# Maps "Country" from the Results CSV to an Array of native/alternative words used in the Calendar CSV.
# This solves the issue of the CSV saying "Brazil" but the Database saying "São Paulo".
race_aliases = {
    "brazil": ["são paulo", "sao paulo", "brasil", "brazil"],
    "emilia-romagna": ["emilia", "romagna"],
    "emilia romagna": ["emilia", "romagna"],
    "mexico": ["mexico", "méxico", "ciudad"],
    "usa": ["united states", "usa", "americas"],
    "great britain": ["british", "britain", "silverstone", "anniversary"],
    "britain": ["british", "britain", "silverstone"],
    "netherlands": ["dutch", "netherlands", "zandvoort"],
    "spain": ["spanish", "spain", "españa", "espana"],
    "austria": ["austrian", "austria", "österreich", "osterreich", "styrian", "steiermark"],
    "hungary": ["hungarian", "hungary", "magyar"],
    "belgium": ["belgian", "belgium", "belgique"],
    "italy": ["italian", "italy", "italia", "tuscan", "tuscany"],
    "china": ["chinese", "china"],
    "japan": ["japanese", "japan"],
    "canada": ["canadian", "canada"],
    "australia": ["australian", "australia"],
    "saudi arabia": ["saudi", "arabia"],
    "qatar": ["qatar"],
    "bahrain": ["bahrain", "sakhir"], 
    "abu dhabi": ["abu dhabi"],
    "las vegas": ["las vegas"],
    "miami": ["miami"],
    "singapore": ["singapore"],
    "monaco": ["monaco"],
    "azerbaijan": ["azerbaijan"]
}

# Maps the verbose engine names found in the results (e.g. "McLaren Mercedes") 
# to the base shortname used in the database (e.g. "McLaren")
team_aliases = {
    "mclaren mercedes": "mclaren",
    "williams mercedes": "williams",
    "aston martin aramco mercedes": "aston martin",
    "haas ferrari": "haas",
    "kick sauber ferrari": "kick sauber", 
    "alpine renault": "alpine",
    "rb honda rbpt": "rb", 
    "racing bulls honda rbpt": "rb", 
    "red bull racing honda rbpt": "red bull racing"
}

# Helper Function: Matches a CSV track to a Database RaceID
def get_race_id(season_year, track_name):
    if not track_name: return None
    track_lower = track_name.lower().strip()
    
    # Grab the list of native keywords (or default to the track name itself)
    aliases = race_aliases.get(track_lower, [track_lower])
    
    for r in race_data:
        # Must match the exact season year to avoid grabbing the wrong year's race ID
        if r['seasonyear'] == season_year:
            gp_lower = gpid_to_name.get(r['gpid'], "")
            
            # If ANY of the alias words are found in the Database GP Name, it's a match!
            for alias in aliases:
                if alias in gp_lower or gp_lower in alias:
                    return r['raceid']
    return None

# Helper Function: Matches a CSV team to a Database TeamID
def get_team_id(team_str):
    if not team_str: return None
    team_lower = team_str.lower().strip()
    
    # Strips out the engine supplier using our mapping dictionary
    clean_team_name = team_aliases.get(team_lower, team_lower)
    
    # Sort teams by length descending to prevent "RB" from accidentally matching "Red Bull" first
    sorted_teams = sorted([t for t in team_data if t['shortname']], key=lambda x: len(x['shortname']), reverse=True)
    
    for t in sorted_teams:
        db_shortname = str(t['shortname']).strip().lower()
        if db_shortname == clean_team_name or db_shortname in clean_team_name:
            return t['teamid']
            
    return None

# Helper Function: Matches a CSV driver to a Database DriverID
def get_driver_id(driver_raw):
    if not driver_raw: return None
    
    # Removes the 3-letter abbreviation suffix often found in F1 CSVs (e.g., "Max Verstappen VER" -> "Max Verstappen")
    clean_name = driver_raw[:-4].strip() if len(driver_raw) > 4 and driver_raw[-3:].isupper() else driver_raw.strip()
    clean_name_lower = clean_name.lower()

    # Direct dictionary lookup first
    if clean_name_lower in driver_map:
        return driver_map[clean_name_lower]
        
    # If a direct match fails (due to spelling/spacing), use Difflib for a "Fuzzy Match"
    # cutoff=0.8 means it needs to be an 80% textual match to succeed
    close_matches = difflib.get_close_matches(clean_name_lower, db_driver_names, n=1, cutoff=0.8)
    if close_matches:
        return driver_map[close_matches[0]]

    return None

# -------------------

results_directory = './raceresults'
all_result_entries = []

# 3. Process the CSV files
for filename in os.listdir(results_directory):
    filename_lower = filename.lower()

    if filename_lower.startswith("formula1_") and "raceresult" in filename_lower:
        filepath = os.path.join(results_directory, filename)
        df = pd.read_csv(filepath)

        # Extract the year from the filename using regex
        year_match = re.search(r'\d{4}', filename)
        if year_match:
            season_year = int(year_match.group())
        else:
            continue
            
        # Standardize the wildly varying column names across different F1 seasons
        col_renames = {
            'Track': 'gpname',
            'Position': 'finishposition',
            'Laps': 'lapscompleted',
            'Time/Retired': 'timeorretired', 'Total Time/Gap/Retirement': 'timeorretired',
            'Points': 'points',
            'Starting Grid': 'startinggrid',
            'Set Fastest Lap': 'fastestlap', 'Fastest Lap': 'fastestlap', ',+1 Pt': 'fastestlap',
            'Fastest Lap Time': 'fastestlaptime'
        }
        # Apply the renaming. lambda c strips hidden spaces from CSV headers
        df = df.rename(columns=lambda c: col_renames.get(str(c).strip(), str(c).strip()))

        # 4. Map the Data Row-by-Row
        for index, row in df.iterrows():
            # Safely extract raw strings using .get() to prevent KeyErrors if a column is missing
            gp_name = str(row.get('gpname', '')).strip() if pd.notna(row.get('gpname')) else ""
            driver_raw = str(row.get('Driver', '')).strip() if pd.notna(row.get('Driver')) else ""
            team_name = str(row.get('Team', '')).strip() if pd.notna(row.get('Team')) else ""
            
            # Fetch Foreign Keys
            driver_id = get_driver_id(driver_raw)
            team_id = get_team_id(team_name)
            race_id = get_race_id(season_year, gp_name)

            # Warning system for unresolved constraints
            if not race_id: print(f"Missing match for Race: {gp_name} ({season_year})")
            if not driver_id: print(f"Missing match for Driver: {driver_raw}")
            if not team_id: print(f"Missing match for Team: {team_name}")

            # Only add the record if all relational keys were successfully found
            if race_id and driver_id and team_id:
                all_result_entries.append({
                    'raceid': race_id,
                    'driverid': driver_id,
                    'teamid': team_id,
                    'startinggrid': row.get('startinggrid'),
                    'finishposition': str(row.get('finishposition', '')) if pd.notna(row.get('finishposition')) else None,
                    'lapscompleted': row.get('lapscompleted'),
                    'timeorretired': str(row.get('timeorretired', '')) if pd.notna(row.get('timeorretired')) else None,
                    'points': float(row.get('points', 0.0)) if pd.notna(row.get('points')) else 0.0,
                    'fastestlap': str(row.get('fastestlap', '')) if pd.notna(row.get('fastestlap')) else None,
                    'fastestlaptime': str(row.get('fastestlaptime', '')) if pd.notna(row.get('fastestlaptime')) else None
                })

if len(all_result_entries) == 0:
    print(f"\nError: No matching records compiled. Check your dataset names.")
    exit()

# 5. Generate Final Dataframe
raceresult_df = pd.DataFrame(all_result_entries)

# Drop any accidental duplicates (A specific driver cannot have two results for the same race)
raceresult_df = raceresult_df.drop_duplicates(subset=['raceid', 'driverid'])

# Generate the primary key 'resultid'
raceresult_df.insert(0, 'resultid', range(1, 1 + len(raceresult_df)))

# Convert Pandas NaN to None for Postgres compatibility
raceresult_df = raceresult_df.astype(object).where(pd.notna(raceresult_df), None)
data_to_insert = raceresult_df.to_dict(orient='records')

# 6. Critical Fix: Pandas Float-to-Int Conversions
# Because some drivers DNF, their "Starting Grid" or "Laps" might be empty.
# Pandas automatically converts entire columns with missing data into floats.
# This loop forces PostgreSQL integer columns back to strictly Python Integers to avoid 22P02 crashes.
int_columns = ['resultid', 'raceid', 'driverid', 'teamid', 'startinggrid', 'lapscompleted']
for record in data_to_insert:
    for col in int_columns:
        if record.get(col) is not None:
            record[col] = int(record[col])

# 7. Insert into Database
print(f"Upserting {len(data_to_insert)} rows into RaceResult...")
response = supabase.table("raceresult").upsert(data_to_insert).execute()
print("Race Result insertion complete!")