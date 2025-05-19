import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import string
import re

# from pprint import pprint # Not used for printing details anymore

BASE_URL = "https://www.basketball-reference.com"


# get_player_page_urls_from_index and parse_player_page functions remain the same
# ... (keep the existing get_player_page_urls_from_index and parse_player_page functions here) ...
def get_player_page_urls_from_index(letter_index_url):
    player_urls = []
    print(f"Fetching player list from: {letter_index_url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(letter_index_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        player_table = soup.find("table", id="players")
        if not player_table:
            print(f"  Could not find player table on {letter_index_url}")
            return []

        tbody = player_table.find("tbody")
        if not tbody:
            print(f"  Could not find tbody in player table on {letter_index_url}")
            return []

        for row in tbody.find_all("tr"):
            first_cell = row.find(["th", "td"])
            if first_cell:
                link_tag = first_cell.find("a")
                if link_tag and link_tag.get("href"):
                    player_urls.append(BASE_URL + link_tag["href"])
        print(f"  Found {len(player_urls)} players on this page.")
    except requests.RequestException as e:
        print(f"  Error fetching {letter_index_url}: {e}")
    except Exception as e:
        print(f"  Error parsing {letter_index_url}: {e}")
    return player_urls


def parse_player_page(player_url):
    print(f"  Scraping player page: {player_url}")
    player_data = {
        "Name": None,
        "Pronunciation": None,
        "Nicknames": None,
        "Position": None,
        "Shoots": None,
        "Height_Imperial": None,
        "Weight_Imperial": None,
        "Height_Metric": None,
        "Weight_Metric": None,
        "Born_Date": None,
        "Born_Location": None,
        "College": None,
        "High School": None,
        "Draft": None,
        "NBA_Debut": None,
        "Career_Length": None,
        "URL": player_url,
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(player_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        info_div = soup.find("div", id="info")
        if not info_div:
            print(f"    Could not find main info div for {player_url}")
            return player_data

        meta_div = info_div.find("div", id="meta")
        if not meta_div:
            print(f"    Could not find meta div for {player_url}")
            return player_data

        h1_tag = meta_div.find("h1")
        content_holder_div = None
        if h1_tag:
            player_data["Name"] = h1_tag.get_text(strip=True)
            content_holder_div = h1_tag.parent
        else:
            potential_holders = meta_div.find_all("div", recursive=False)
            for div_child in potential_holders:
                if "media-item" not in div_child.get("class", []):
                    content_holder_div = div_child
                    h1_in_holder = content_holder_div.find("h1")
                    if h1_in_holder:
                        player_data["Name"] = h1_in_holder.get_text(strip=True)
                    break
            if not content_holder_div:
                content_holder_div = meta_div

        if not content_holder_div:
            print(
                f"    Could not find content holder div within #meta for {player_url}"
            )
            if not player_data["Name"] and info_div.find("h1"):
                player_data["Name"] = info_div.find("h1").get_text(strip=True)
            return player_data

        if not player_data["Name"] and content_holder_div.find("h1"):
            player_data["Name"] = content_holder_div.find("h1").get_text(strip=True)

        paragraphs = content_holder_div.find_all("p", recursive=False)

        found_pronunciation = False
        found_nicknames = False
        found_ht_wt = False

        for p_tag in paragraphs:
            strong_tag = p_tag.find("strong")
            p_text_full = p_tag.get_text(strip=True).replace("\xa0", " ")

            if strong_tag:
                label = strong_tag.get_text(strip=True)
                value_text = (
                    "".join(
                        (
                            sibling.get_text(strip=True)
                            if hasattr(sibling, "get_text")
                            else str(sibling).strip()
                        )
                        for sibling in strong_tag.next_siblings
                    )
                    .strip()
                    .lstrip(":")
                    .strip()
                )

                if "Pronunciation" in label and not found_pronunciation:
                    player_data["Pronunciation"] = value_text
                    found_pronunciation = True
                elif "Position:" in label or "Shoots:" in label:
                    for part in p_text_full.split("▪"):
                        if "Position:" in part:
                            player_data["Position"] = part.split("Position:", 1)[
                                -1
                            ].strip()
                        elif "Shoots:" in part:
                            player_data["Shoots"] = part.split("Shoots:", 1)[-1].strip()
                elif "Born:" in label:
                    born_date_span = p_tag.find("span", id="necro-birth")
                    if born_date_span:
                        player_data["Born_Date"] = born_date_span.get_text(strip=True)

                    location_parts = []
                    start_node = born_date_span if born_date_span else strong_tag
                    for elem in start_node.next_siblings:
                        if elem.name == "span" and "f-i" in elem.get("class", []):
                            break
                        location_parts.append(
                            elem.get_text(strip=True)
                            if hasattr(elem, "get_text")
                            else str(elem).strip()
                        )
                    location = " ".join(filter(None, location_parts)).strip()
                    if location.lower().startswith("in "):
                        location = location[3:]
                    player_data["Born_Location"] = location
                elif "College:" in label or "Colleges:" in label:
                    college_links = p_tag.find_all("a")
                    player_data["College"] = (
                        ", ".join([a.get_text(strip=True) for a in college_links])
                        if college_links
                        else value_text
                    )
                elif "High School:" in label:
                    hs_parts = [
                        (
                            elem.get_text(strip=True)
                            if hasattr(elem, "get_text")
                            else str(elem).strip()
                        )
                        for elem in strong_tag.next_siblings
                    ]
                    player_data["High School"] = (
                        " ".join(filter(None, hs_parts)).lstrip(":").strip()
                    )
                elif "Draft:" in label:
                    player_data["Draft"] = value_text
                elif "NBA Debut:" in label:
                    debut_link = p_tag.find("a")
                    player_data["NBA_Debut"] = (
                        debut_link.get_text(strip=True) if debut_link else value_text
                    )
                elif "Career Length:" in label:
                    player_data["Career_Length"] = value_text

            else:
                if (
                    not found_nicknames
                    and p_text_full.startswith("(")
                    and p_text_full.endswith(")")
                    and "Age:" not in p_text_full
                ):
                    player_data["Nicknames"] = p_text_full
                    found_nicknames = True
                elif not found_ht_wt:
                    match_ht_wt = re.search(
                        r'(\d+-\d+|\d+\'\d+"?)\s*,\s*(\d+lb)\s*\(([^,]+cm)\s*,\s*([^)]+kg)\)',
                        p_text_full,
                    )
                    if match_ht_wt:
                        player_data["Height_Imperial"] = match_ht_wt.group(1)
                        player_data["Weight_Imperial"] = match_ht_wt.group(2)
                        player_data["Height_Metric"] = match_ht_wt.group(3).strip()
                        player_data["Weight_Metric"] = match_ht_wt.group(4).strip()
                        found_ht_wt = True

    except requests.RequestException as e:
        print(f"    Error fetching player page {player_url}: {e}")
    except Exception as e:
        print(f"    Error parsing player page {player_url}: {e}")
        import traceback

        traceback.print_exc()

    return player_data


def main():
    all_player_data = []
    letters_to_scrape = string.ascii_lowercase
    total_players_scraped = 0  # Keep track of overall players for final report
    max_players_per_letter = 100  # Set the limit per letter
    REQUEST_DELAY = 3  # seconds, as per robots.txt

    for letter in letters_to_scrape:
        index_url = f"{BASE_URL}/players/{letter}/"
        player_links = get_player_page_urls_from_index(index_url)
        # Delay after fetching the index page, before processing its players
        if (
            player_links
        ):  # Only sleep if we actually got links and will make more requests
            time.sleep(REQUEST_DELAY)

        players_scraped_this_letter = 0  # Reset counter for each new letter

        for i, player_url in enumerate(player_links):
            if players_scraped_this_letter >= max_players_per_letter:
                print(
                    f"  Reached limit of {max_players_per_letter} players for letter '{letter.upper()}'. Moving to next letter."
                )
                break  # Break out of the player loop for the current letter

            player_details = parse_player_page(player_url)
            all_player_data.append(player_details)
            players_scraped_this_letter += 1
            total_players_scraped += 1
            print(
                f"    Scraped player #{players_scraped_this_letter} for letter '{letter.upper()}' (Total: {total_players_scraped}): {player_details.get('Name', 'Unknown Name')}"
            )
            # Delay after fetching each player page, but only if we are not at the letter limit yet
            if (
                players_scraped_this_letter < max_players_per_letter
                and i < len(player_links) - 1
            ):  # also check if it's not the last player in the list
                time.sleep(REQUEST_DELAY)

        print(
            f"Finished letter {letter.upper()}. Scraped {players_scraped_this_letter} players for this letter."
        )
        # The delay after get_player_page_urls_from_index handles the pause before the *next* letter's index fetch.
        # No extra sleep explicitly needed here unless there was non-request work between letters.

    df = pd.DataFrame(all_player_data)

    column_order = [
        "Name",
        "Pronunciation",
        "Nicknames",
        "Position",
        "Shoots",
        "Height_Imperial",
        "Weight_Imperial",
        "Height_Metric",
        "Weight_Metric",
        "Born_Date",
        "Born_Location",
        "College",
        "High School",
        "Draft",
        "NBA_Debut",
        "Career_Length",
        "URL",
    ]
    for col in column_order:
        if col not in df.columns:
            df[col] = None
    df = df[column_order]

    csv_filename = (
        f"basketball_reference_players_max{max_players_per_letter}_per_letter.csv"
    )
    try:
        df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
        print(f"\nSuccessfully saved data to {csv_filename}")
    except Exception as e:
        print(f"\nError saving to CSV: {e}")

    print(f"\nScraped a total of {total_players_scraped} players.")
    if not df.empty:
        print("\nFirst 5 rows of data:")
        print(df.head().to_string())
    else:
        print("No data was scraped.")

    return df


if __name__ == "__main__":
    df = main()
