import pandas as pd
import re

# Compiled Regex Patterns
RE_SUFFIXES = re.compile(r"\b(jr|sr|ii|iii|iv|v)\.?\b", re.IGNORECASE)
RE_PUNCTUATION = re.compile(r"[^\w\s']")
RE_MULTIPLE_SPACES = re.compile(r"\s+")

RE_BORN_PREFIX = re.compile(
    r"^in\s*([ÂÀ]\s*)?", re.IGNORECASE
)  # Handles "in", "inÂ", "inÀ", etc.
RE_COMMA_SPACE = re.compile(r",\s*")

RE_MONTH_DAY_YEAR = re.compile(r"(\w+)\s+(\d{1,2}),?\s*(\d{4})", re.IGNORECASE)
RE_YYYY_MM_DD = re.compile(r"\d{4}-\d{2}-\d{2}")
RE_MM_DD_YYYY = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")

RE_HEIGHT_MONTH_WORDS = re.compile(r"\b(month|months|mo|mos|m)\b", re.IGNORECASE)
RE_HEIGHT_MONTH_ABBRS = re.compile(
    r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", re.IGNORECASE
)
RE_HEIGHT_FT_IN = re.compile(r"(\d+)['\s]*[-\s]*(\d+)")
RE_HEIGHT_FT_ONLY = re.compile(r"(\d+)['\s]*(?:ft|feet)?")

# Name replacements
NAME_REPLACEMENTS = {
    "shaquille oneal": "shaquille o'neal",
}

# Month name to number mapping (lowercase keys)
MONTH_TO_NUM_MAP = {
    "january": "01",
    "february": "02",
    "march": "03",
    "april": "04",
    "may": "05",
    "june": "06",
    "july": "07",
    "august": "08",
    "september": "09",
    "october": "10",
    "november": "11",
    "december": "12",
    "jan": "01",
    "feb": "02",
    "mar": "03",
    "apr": "04",
    "jun": "06",
    "jul": "07",
    "aug": "08",
    "sep": "09",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}

# Excel date to height part conversions
EXCEL_DATE_TO_HEIGHT_CONVERSIONS = {
    "jan": "1",
    "feb": "2",
    "mar": "3",
    "apr": "4",
    "may": "5",
    "jun": "6",
    "jul": "7",
    "aug": "8",
    "sep": "9",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}


def normalize_name(name):
    if pd.isna(name):
        return ""

    name_str = str(name).strip()

    if name_str.isupper():
        name_str = " ".join(word.capitalize() for word in name_str.split())

    name_str = name_str.lower()
    name_str = RE_SUFFIXES.sub("", name_str)

    for variant, standard in NAME_REPLACEMENTS.items():
        name_str = name_str.replace(variant, standard)

    name_str = RE_PUNCTUATION.sub("", name_str)
    name_str = RE_MULTIPLE_SPACES.sub(" ", name_str).strip()

    return name_str


def fuzzy_name_match(espn_name, br_names_normalized_parts_list, threshold=0.8):
    espn_normalized = normalize_name(espn_name)
    if not espn_normalized:
        return None

    espn_parts = espn_normalized.split()
    if not espn_parts:  # Handles names that normalize to empty or whitespace only
        return None

    min_match_score = len(espn_parts) * threshold

    best_match_name = None
    highest_score = -1

    for original_br_name, br_parts_set in br_names_normalized_parts_list:
        # Ensure br_parts_set is not empty to avoid issues if a BR name normalized to empty
        if not br_parts_set:
            continue

        matches = sum(1 for part in espn_parts if part in br_parts_set)

        # Prefer exact subset matches first if they meet threshold
        if matches >= min_match_score:
            # Simple score: ratio of matched ESPN parts to total ESPN parts
            # Could be enhanced with Jaccard index or other similarity scores
            current_score = matches / len(espn_parts)

            # If this match is better than previous ones, take it.
            # This prioritizes names that have a higher proportion of their parts matched.
            if current_score > highest_score:
                highest_score = current_score
                best_match_name = original_br_name
                # If it's a perfect match of all parts, take it immediately
                if current_score == 1.0:
                    return best_match_name

    return best_match_name


def format_born_location(location):
    if pd.isna(location):
        return ""

    location_str = str(location).strip()
    location_str = RE_BORN_PREFIX.sub("", location_str)
    location_str = RE_MULTIPLE_SPACES.sub(" ", location_str)
    location_str = RE_COMMA_SPACE.sub(", ", location_str)

    return location_str.strip()


def standardize_date_format(date_str):
    if pd.isna(date_str):
        return ""

    date_str = str(date_str).strip()

    match = RE_MONTH_DAY_YEAR.match(date_str)
    if match:
        month_name, day, year = match.groups()
        month_num = MONTH_TO_NUM_MAP.get(month_name.lower())
        if month_num:
            return f"{year}-{month_num}-{day.zfill(2)}"

    if RE_YYYY_MM_DD.match(date_str):
        return date_str

    match = RE_MM_DD_YYYY.match(date_str)
    if match:
        month, day, year = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    return date_str


def standardize_height_imperial(height_str):
    if pd.isna(height_str):
        return ""

    height_str = str(height_str).strip()

    for month_abbr, month_num in EXCEL_DATE_TO_HEIGHT_CONVERSIONS.items():
        # Pattern for "-Jun" -> "-6"
        height_str = re.sub(
            f"-{month_abbr}", f"-{month_num}", height_str, flags=re.IGNORECASE
        )
        # Pattern for "Jul-" -> "7-"
        height_str = re.sub(
            f"{month_abbr}-", f"{month_num}-", height_str, flags=re.IGNORECASE
        )

    height_str = RE_HEIGHT_MONTH_WORDS.sub("", height_str)
    height_str = RE_HEIGHT_MONTH_ABBRS.sub(
        "", height_str
    )  # Clean up any loose month names

    match = RE_HEIGHT_FT_IN.search(height_str)
    if match:
        feet, inches = match.groups()
        return f"{feet}-{inches}"

    match = RE_HEIGHT_FT_ONLY.search(height_str)
    if match:
        feet = match.group(1)
        return f"{feet}-0"

    height_str = RE_MULTIPLE_SPACES.sub(
        "", height_str
    )  # Remove all spaces if not matched

    return height_str.strip()


def _create_ordered_merged_player_entry(br_player_info, espn_row_data):
    player_data = br_player_info.copy()
    if "Normalized_Name" in player_data:  # Internal field
        del player_data["Normalized_Name"]

    player_data["ESPN_Rank"] = espn_row_data["RK"]
    player_data["ESPN_Points"] = espn_row_data["PTS"]

    ordered_entry = {}
    ordered_entry["ESPN_Rank"] = player_data["ESPN_Rank"]
    ordered_entry["Name"] = player_data["Name"]
    ordered_entry["ESPN_Points"] = player_data["ESPN_Points"]

    for key, value in player_data.items():
        if key not in ordered_entry:
            ordered_entry[key] = value

    return ordered_entry


def clean_and_merge_player_data(br_csv_path, espn_csv_path):
    try:
        br_df = pd.read_csv(br_csv_path)
        espn_df = pd.read_csv(espn_csv_path)
        print(f"Loaded {len(br_df)} players from Basketball Reference")
        print(f"Loaded {len(espn_df)} players from ESPN leaders")
    except Exception as e:
        print(f"Error loading CSV files: {e}")
        return [], [], {}

    print("\nProcessing Basketball Reference data...")
    br_df["Born_Location"] = br_df["Born_Location"].apply(format_born_location)
    br_df["Born_Date"] = br_df["Born_Date"].apply(standardize_date_format)
    br_df["NBA_Debut"] = br_df["NBA_Debut"].apply(standardize_date_format)
    br_df["Height_Imperial"] = br_df["Height_Imperial"].apply(
        standardize_height_imperial
    )

    columns_to_remove_br = ["Pronunciation", "High School", "College", "Draft", "URL"]
    br_df = br_df.drop(columns=columns_to_remove_br, errors="ignore")

    br_df["Normalized_Name"] = br_df["Name"].apply(normalize_name)

    br_lookup = {}
    for _, row in br_df.iterrows():
        normalized = row["Normalized_Name"]
        if (
            normalized and normalized not in br_lookup
        ):  # Store first encountered for a given normalized name
            br_lookup[normalized] = row.to_dict()
    print(f"Created lookup for {len(br_lookup)} unique normalized BR names")

    # Pre-process BR names for fuzzy matching
    br_names_for_fuzzy_prepped = []
    # Use unique names from br_df to avoid redundant processing for fuzzy list
    # If 'Name' can be NaN, ensure it's handled or dropped before unique()
    unique_br_names = br_df["Name"].dropna().unique()
    for name in unique_br_names:
        normalized_name_str = normalize_name(name)
        if normalized_name_str:  # Ensure not empty
            br_names_for_fuzzy_prepped.append((name, set(normalized_name_str.split())))

    print("\nProcessing ESPN scoring leaders...")
    espn_df["Normalized_ESPN_Name"] = espn_df["Player"].apply(normalize_name)

    merged_data = []
    unmatched_players = []
    exact_matches = 0
    fuzzy_matches = 0

    for _, espn_row in espn_df.iterrows():
        espn_normalized = espn_row["Normalized_ESPN_Name"]
        br_player_info = None

        if espn_normalized and espn_normalized in br_lookup:
            br_player_info = br_lookup[espn_normalized]
            exact_matches += 1
        else:
            fuzzy_match_original_br_name = fuzzy_name_match(
                espn_row["Player"], br_names_for_fuzzy_prepped
            )
            if fuzzy_match_original_br_name:
                # We have the original BR name, get its normalized form to lookup in br_lookup
                normalized_fuzzy_br_name = normalize_name(fuzzy_match_original_br_name)
                if normalized_fuzzy_br_name in br_lookup:
                    br_player_info = br_lookup[normalized_fuzzy_br_name]
                    fuzzy_matches += 1

        if br_player_info:
            ordered_entry = _create_ordered_merged_player_entry(
                br_player_info, espn_row
            )
            merged_data.append(ordered_entry)
        else:
            unmatched_players.append(
                {
                    "ESPN_Player": espn_row["Player"],
                    "ESPN_Rank": espn_row["RK"],
                    "ESPN_Points": espn_row["PTS"],
                    "Normalized_Name": espn_normalized,  # For debugging unmatched
                }
            )

    stats = {
        "total_espn_players": len(espn_df),
        "total_br_players": len(br_df),  # Could be len(br_lookup) for unique normalized
        "exact_matches": exact_matches,
        "fuzzy_matches": fuzzy_matches,
        "total_matches": len(merged_data),
        "unmatched": len(unmatched_players),
    }
    if len(espn_df) > 0:
        stats["match_percentage"] = (len(merged_data) / len(espn_df)) * 100
    else:
        stats["match_percentage"] = 0.0

    return merged_data, unmatched_players, stats


def print_merge_results(merged_data, unmatched_players, stats):
    print("\n" + "=" * 60)
    print("MERGE RESULTS SUMMARY")
    print("=" * 60)
    print(f"Total ESPN players: {stats['total_espn_players']}")
    print(f"Total BR players (raw): {stats['total_br_players']}")
    print(f"Exact matches: {stats['exact_matches']}")
    print(f"Fuzzy matches: {stats['fuzzy_matches']}")
    print(f"Total successful matches: {stats['total_matches']}")
    print(f"Unmatched players: {stats['unmatched']}")
    print(f"Match success rate: {stats['match_percentage']:.1f}%")

    if merged_data:
        print("\n" + "-" * 60)
        print("SUCCESSFULLY MATCHED PLAYERS (Sorted by ESPN Rank):")
        print("-" * 60)
        # Sort merged_data by 'ESPN_Rank' for printing
        for player in sorted(merged_data, key=lambda x: x["ESPN_Rank"]):
            br_name = player.get("Name", "N/A")
            print(
                f"{player['ESPN_Rank']:2d}. {br_name:<30} ({player['ESPN_Points']:,} pts)"
            )

    if unmatched_players:
        print("\n" + "-" * 60)
        print("UNMATCHED PLAYERS (Sorted by ESPN Rank):")
        print("-" * 60)
        for player in sorted(unmatched_players, key=lambda x: x["ESPN_Rank"]):
            print(
                f"{player['ESPN_Rank']:2d}. {player['ESPN_Player']:<30} ({player['ESPN_Points']:,} pts) (Normalized: {player['Normalized_Name']})"
            )


def save_merged_data(merged_data_list, output_path):
    if not merged_data_list:
        print("No merged data to save.")
        return False

    df = pd.DataFrame(merged_data_list)

    # Define desired column order for the CSV
    # Start with specific ESPN/BR columns, then all others dynamically
    # This ensures Name comes from BR, and ESPN Rank/Points are prominent
    cols_ordered = ["ESPN_Rank", "Name", "ESPN_Points"]

    # Get other columns from the DataFrame, excluding those already in cols_ordered
    # and ensuring they exist in the DataFrame (though df.columns guarantees this)
    other_cols = [col for col in df.columns if col not in cols_ordered]
    final_columns = cols_ordered + other_cols

    # Reorder DataFrame columns for consistent output
    df = df[final_columns]

    try:
        df.to_csv(output_path, index=False)
        print(f"\nMerged data saved to: {output_path}")
        return True
    except Exception as e:
        print(f"Error saving merged data to CSV: {e}")
        return False


def main():
    br_csv_path = "basketball_reference_players_max100_per_letter.csv"
    espn_csv_path = "espn_nba_leaders_pts.csv"
    output_path = "merged_players_data.csv"

    print("Starting player data merge process...")
    merged_data, unmatched_players, stats = clean_and_merge_player_data(
        br_csv_path, espn_csv_path
    )

    print_merge_results(merged_data, unmatched_players, stats)

    if merged_data:
        save_successful = save_merged_data(merged_data, output_path)

        if save_successful:
            print("\n" + "-" * 60)
            print("SAMPLE OF MERGED DATA (first player from sorted list):")
            print("-" * 60)
            # Use the first player from the rank-sorted list for sample display
            sample_player = sorted(merged_data, key=lambda x: x["ESPN_Rank"])[0]
            for key, value in sample_player.items():
                # Filter out any potential internal fields if they were to be added later
                if key not in [
                    "Normalized_Name",
                    "ESPN_Player_Name",
                    "Match_Type",
                    "Matched_BR_Name",
                ]:
                    print(f"  {key}: {value}")

    return merged_data, unmatched_players, stats


if __name__ == "__main__":
    main()
