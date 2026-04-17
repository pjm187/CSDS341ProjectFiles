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


def get_driver_id(driver_raw):
    """Driver of the Day CSVs use full names only (no 3-letter suffix),
    so we go straight to direct lookup then fuzzy match on failure."""
    if not driver_raw:
        return None
    clean_name = driver_raw.strip().lower()

    if clean_name in driver_map:
        return driver_map[clean_name]

    close_matches = difflib.get_close_matches(clean_name, db_driver_names, n=1, cutoff=0.8)
    if close_matches:
        return driver_map[close_matches[0]]
    return None

dotd_directory = './driveroftheday'
all_dotd_entries = []

# Maps "1st Place" → rank integer 1, etc.
rank_map = {
    '1st Place': 1,
    '2nd Place': 2,
    '3rd Place': 3,
    '4th Place': 4,
    '5th Place': 5,
}

for filename in os.listdir(dotd_directory):
    filename_lower = filename.lower()

    if not (filename_lower.startswith("formula1_") and "driveroftheday" in filename_lower):
        continue

    filepath = os.path.join(dotd_directory, filename)

    year_match = re.search(r'\d{4}', filename)
    if not year_match:
        print(f"Skipping {filename}: could not extract year.")
        continue
    season_year = int(year_match.group())

    df = pd.read_csv(filepath)
    # Strip hidden whitespace from column headers
    df.columns = [c.strip() for c in df.columns]

    print(f"\nProcessing: {filename} ({season_year}) — {len(df)} races")

    for _, row in df.iterrows():
        track_name = str(row.get('Track', '')).strip() if pd.notna(row.get('Track')) else ""
        race_id    = get_race_id(season_year, track_name)

        if not race_id:
            print(f"  [WARN] No race match: '{track_name}' ({season_year})")
            continue

        # Unpack the 5 ranked drivers from the wide columns into individual rows
        for place_col, rank in rank_map.items():
            pct_col    = f"{place_col}(%)"
            driver_raw = str(row.get(place_col, '')).strip() if pd.notna(row.get(place_col)) else ""
            pct_raw    = row.get(pct_col)

            if not driver_raw:
                continue

            driver_id = get_driver_id(driver_raw)

            if not driver_id:
                print(f"  [WARN] No driver match: '{driver_raw}' (Rank {rank}, {track_name} {season_year})")
                continue

            vote_pct = float(pct_raw) if pd.notna(pct_raw) else None

            all_dotd_entries.append({
                'raceid':          race_id,
                'rank':            rank,
                'driverid':        driver_id,
                'votepercentage':  vote_pct,
            })

if not all_dotd_entries:
    print("\nError: No matching records compiled. Check directory name and CSV filenames.")
    exit(1)

dotd_df = pd.DataFrame(all_dotd_entries)

dotd_df = dotd_df.drop_duplicates(subset=['raceid', 'rank'])

dotd_df.insert(0, 'dotdvoteid', range(1, 1 + len(dotd_df)))

dotd_df = dotd_df.astype(object).where(pd.notna(dotd_df), None)

data_to_insert = dotd_df.to_dict(orient='records')

int_columns = ['dotdvoteid', 'raceid', 'rank', 'driverid']
for record in data_to_insert:
    for col in int_columns:
        if record.get(col) is not None:
            try:
                record[col] = int(record[col])
            except (ValueError, TypeError):
                record[col] = None

print(f"\nUpserting {len(data_to_insert)} rows into driveroftheday...")
response = supabase.table("driveroftheday").upsert(data_to_insert).execute()
print("Driver of the Day insertion complete!")