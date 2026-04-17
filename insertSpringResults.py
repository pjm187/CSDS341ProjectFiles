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
    "rb honda rbpt":                  "rb",
    "red bull racing rbpt":           "red bull racing",
    "alphatauri rbpt":                "alphatauri",
    "alfa romeo ferrari":             "alfa romeo",
    "mclaren mercedes":               "mclaren",
    "williams mercedes":              "williams",
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

sprint_directory  = './sprintresults'
all_sprint_entries = []

for filename in os.listdir(sprint_directory):
    filename_lower = filename.lower()

    # Match sprint result files — exclude sprint qualifying files
    if not (filename_lower.startswith("formula1_") and
            "sprint" in filename_lower and
            "result" in filename_lower and
            "qualifying" not in filename_lower):
        continue

    filepath = os.path.join(sprint_directory, filename)

    year_match = re.search(r'\d{4}', filename)
    if not year_match:
        print(f"Skipping {filename}: could not extract year.")
        continue
    season_year = int(year_match.group())

    df = pd.read_csv(filepath)

    col_renames = {
        'Track':          'track',
        'Position':       'finishposition',
        'Pos':            'finishposition',
        'Starting Grid':  'startinggrid',
        'Laps':           'lapscompleted',
        'Time/Retired':   'timeorretired',
        'Total Time/Gap/Retirement': 'timeorretired',
        'Points':         'points',
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
            all_sprint_entries.append({
                'raceid':          race_id,
                'driverid':        driver_id,
                'teamid':          team_id,
                'startinggrid':    row.get('startinggrid'),
                'finishposition':  str(row.get('finishposition', '')) if pd.notna(row.get('finishposition')) else None,
                'lapscompleted':            row.get('lapscompleted'),
                'timeorretired':   str(row.get('timeorretired', '')) if pd.notna(row.get('timeorretired')) else None,
                'points':          float(row.get('points', 0.0)) if pd.notna(row.get('points')) else 0.0,
            })

if not all_sprint_entries:
    print("\nError: No matching records compiled. Check directory name and CSV filenames.")
    exit(1)

sprint_df = pd.DataFrame(all_sprint_entries)

sprint_df = sprint_df.drop_duplicates(subset=['raceid', 'driverid'])

sprint_df.insert(0, 'sprintid', range(1, 1 + len(sprint_df)))

sprint_df = sprint_df.astype(object).where(pd.notna(sprint_df), None)

data_to_insert = sprint_df.to_dict(orient='records')

int_columns = ['sprintid', 'raceid', 'driverid', 'teamid', 'startinggrid', 'lapscompleted', 'points']
for record in data_to_insert:
    for col in int_columns:
        if record.get(col) is not None:
            try:
                record[col] = int(record[col])
            except (ValueError, TypeError):
                record[col] = None

print(f"\nUpserting {len(data_to_insert)} rows into sprintresult...")
response = supabase.table("sprintresult").upsert(data_to_insert).execute()
print("Sprint Result insertion complete!")