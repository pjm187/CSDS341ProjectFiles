import os
import re
import pandas as pd
import difflib
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Error: SUPABASE_URL or SUPABASE_KEY not found in .env file!")
    exit(1)

supabase: Client = create_client(url, key)

driver_data = supabase.table("driver").select("driverid, name").execute().data
team_data   = supabase.table("teamseason").select("teamid, shortname").execute().data
gp_data     = supabase.table("grandprix").select("gpid, gpname").execute().data
race_data   = supabase.table("race").select("raceid, seasonyear, gpid").execute().data

gpid_to_name = {row['gpid']: str(row['gpname']).strip().lower() for row in gp_data}

driver_map      = {str(d['name']).strip().lower(): d['driverid'] for d in driver_data}
db_driver_names = list(driver_map.keys())

race_aliases = {
    "brazil":           ["são paulo", "sao paulo", "brasil", "brazil"],
    "emilia-romagna":   ["emilia", "romagna"],
    "emilia romagna":   ["emilia", "romagna"],
    "mexico":           ["mexico", "méxico", "ciudad"],
    "usa":              ["united states", "usa", "americas"],
    "united states":    ["united states", "usa", "americas"],
    "great britain":    ["british", "britain", "silverstone", "anniversary"],
    "britain":          ["british", "britain", "silverstone"],
    "netherlands":      ["dutch", "netherlands", "zandvoort"],
    "spain":            ["spanish", "spain", "españa", "espana"],
    "austria":          ["austrian", "austria", "österreich", "osterreich", "styrian", "steiermark"],
    "hungary":          ["hungarian", "hungary", "magyar"],
    "belgium":          ["belgian", "belgium", "belgique"],
    "italy":            ["italian", "italy", "italia", "tuscan", "tuscany"],
    "china":            ["chinese", "china"],
    "japan":            ["japanese", "japan"],
    "canada":           ["canadian", "canada"],
    "australia":        ["australian", "australia"],
    "saudi arabia":     ["saudi", "arabia"],
    "qatar":            ["qatar"],
    "bahrain":          ["bahrain", "sakhir"],
    "abu dhabi":        ["abu dhabi"],
    "las vegas":        ["las vegas"],
    "miami":            ["miami"],
    "singapore":        ["singapore"],
    "monaco":           ["monaco"],
    "azerbaijan":       ["azerbaijan"],
    "france":           ["french", "france", "paul ricard"],
}

team_aliases = {

    "red bull racing honda rbpt":     "red bull racing",  
    "racing bulls honda rbpt":        "rb",
    "kick sauber ferrari":            "kick sauber",       
    "williams mercedes":              "williams", 
    "rb honda rbpt":                  "rb",
    "red bull racing rbpt":           "red bull racing",
    "alphatauri rbpt":                "alphatauri",
    "alfa romeo ferrari":             "alfa romeo",
    "mclaren mercedes":               "mclaren",
    "aston martin aramco mercedes":   "aston martin",
    "haas ferrari":                   "haas",
    "alpine renault":                 "alpine",
}

def get_race_id(season_year, track_name):
    if not track_name:
        return None
    track_lower = track_name.lower().strip()
    aliases = race_aliases.get(track_lower, [track_lower])

    for r in race_data:
        if r['seasonyear'] == season_year:
            gp_lower = gpid_to_name.get(r['gpid'], "")
            for alias in aliases:
                if alias in gp_lower or gp_lower in alias:
                    return r['raceid']
    return None


def get_team_id(team_str):
    if not team_str:
        return None
    team_lower = team_str.lower().strip()
    clean_team_name = team_aliases.get(team_lower, team_lower)

    sorted_teams = sorted(
        [t for t in team_data if t['shortname']],
        key=lambda x: len(x['shortname']),
        reverse=True
    )
    for t in sorted_teams:
        db_shortname = str(t['shortname']).strip().lower()
        if db_shortname == clean_team_name or db_shortname in clean_team_name:
            return t['teamid']
    return None


def get_driver_id(driver_raw):
    if not driver_raw:
        return None
    clean_name = (
        driver_raw[:-4].strip()
        if len(driver_raw) > 4 and driver_raw[-3:].isupper()
        else driver_raw.strip()
    )
    clean_name_lower = clean_name.lower()

    if clean_name_lower in driver_map:
        return driver_map[clean_name_lower]

    close_matches = difflib.get_close_matches(clean_name_lower, db_driver_names, n=1, cutoff=0.8)
    if close_matches:
        return driver_map[close_matches[0]]
    return None


def normalize_lap_time(raw):
    if pd.isna(raw) or str(raw).strip() in ("", "nan"):
        return None
    return str(raw).strip()

sprint_qual_directory = './sprintqualifyingresults'
all_sq_entries        = []

for filename in os.listdir(sprint_qual_directory):
    filename_lower = filename.lower()

    if not (filename_lower.startswith("formula1_") and
            "sprint" in filename_lower and
            "qualifying" in filename_lower):
        continue

    filepath = os.path.join(sprint_qual_directory, filename)

    year_match = re.search(r'\d{4}', filename)
    if not year_match:
        print(f"Skipping {filename}: could not extract year.")
        continue
    season_year = int(year_match.group())

    df = pd.read_csv(filepath)

    if 'Q1' not in df.columns and 'SQ1' not in df.columns:
        print(f"Skipping {filename}: no SQ1/Q1 session time columns found. "
              f"(The 2021 format is incompatible with this schema.)")
        continue

    col_renames = {
        'Track':   'track',
        'Circuit': 'track',
        'Position': 'position',
        'Pos':      'position',
        'Q1':       'sq1time',
        'SQ1':      'sq1time',
        'Q1 Time':  'sq1time',
        'SQ1 Time': 'sq1time',
        'Q2':       'sq2time',
        'SQ2':      'sq2time',
        'Q2 Time':  'sq2time',
        'SQ2 Time': 'sq2time',
        'Q3':       'sq3time',
        'SQ3':      'sq3time',
        'Q3 Time':  'sq3time',
        'SQ3 Time': 'sq3time',
        'Laps':     'laps',
    }
    df = df.rename(columns=lambda c: col_renames.get(str(c).strip(), str(c).strip()))

    print(f"\nProcessing: {filename} ({season_year}) — {len(df)} rows")

    for _, row in df.iterrows():
        track_name = str(row.get('track', '')).strip()  if pd.notna(row.get('track'))  else ""
        driver_raw = str(row.get('Driver', '')).strip() if pd.notna(row.get('Driver')) else ""
        team_name  = str(row.get('Team', '')).strip()   if pd.notna(row.get('Team'))   else ""

        race_id   = get_race_id(season_year, track_name)
        driver_id = get_driver_id(driver_raw)
        team_id   = get_team_id(team_name)

        if not race_id:   print(f"  [WARN] No race match:   '{track_name}' ({season_year})")
        if not driver_id: print(f"  [WARN] No driver match: '{driver_raw}'")
        if not team_id:   print(f"  [WARN] No team match:   '{team_name}'")

        if race_id and driver_id and team_id:
            all_sq_entries.append({
                'raceid':   race_id,
                'driverid': driver_id,
                'teamid':   team_id,
                'position': row.get('position'),
                'sq1time':  normalize_lap_time(row.get('sq1time')),
                'sq2time':  normalize_lap_time(row.get('sq2time')),
                'sq3time':  normalize_lap_time(row.get('sq3time')),
                'laps':     row.get('laps'),
            })

if not all_sq_entries:
    print("\nError: No matching records compiled. Check directory name and CSV filenames.")
    exit(1)

sq_df = pd.DataFrame(all_sq_entries)

sq_df = sq_df.drop_duplicates(subset=['raceid', 'driverid'])

sq_df.insert(0, 'squalid', range(1, 1 + len(sq_df)))

sq_df = sq_df.astype(object).where(pd.notna(sq_df), None)

data_to_insert = sq_df.to_dict(orient='records')

int_columns = ['squalid', 'raceid', 'driverid', 'teamid', 'position', 'laps']
for record in data_to_insert:
    for col in int_columns:
        if record.get(col) is not None:
            try:
                record[col] = int(record[col])
            except (ValueError, TypeError):
                record[col] = None

print(f"\nUpserting {len(data_to_insert)} rows into sprintqualifyingresult...")
response = supabase.table("sprintqualifyingresult").upsert(data_to_insert).execute()
print("Sprint Qualifying Result insertion complete!")