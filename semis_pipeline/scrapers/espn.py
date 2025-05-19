# espn.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time  # Added for potential future delay if needed per page, though not used for this single page.

# Constants (if any specific to ESPN, like base URL if you expand it)
# ESPN_BASE_URL = "https://www.espn.com"


def scrape_espn_nba_leaders(url="https://www.espn.com/nba/history/leaders"):
    """
    Scrapes the NBA leaders table from ESPN for RK, Player, and PTS columns.

    Args:
        url (str): The URL of the ESPN NBA leaders page.

    Returns:
        pandas.DataFrame or None: The scraped data as a DataFrame, or None on failure.
    """
    print(f"Scraping ESPN NBA leaders from: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    scraped_data = []

    try:
        # Add a small delay before hitting the ESPN server
        time.sleep(1)  # Be respectful

        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        soup = BeautifulSoup(response.content, "html.parser")

        stats_table = soup.find("table", class_="tablehead")
        if not stats_table:
            print("  Could not find the main stats table (class 'tablehead') on ESPN.")
            return None

        # Find header row - ESPN might change classes, so be a bit flexible
        header_row = stats_table.find("tr", class_="colhead")
        if not header_row:  # Fallback if 'colhead' class isn't found
            header_row = stats_table.find("tr")
            if not header_row or not header_row.find_all(
                ["th", "td"]
            ):  # Ensure it looks like a header
                print("  Could not identify a suitable header row in the ESPN table.")
                return None

        headers_th = header_row.find_all(["th", "td"])  # Some headers might be td
        header_names = [th.get_text(strip=True).upper() for th in headers_th]

        # Ensure we have the columns we need
        desired_cols = {"RK", "PLAYER", "PTS"}
        col_indices = {}
        for col in desired_cols:
            try:
                col_indices[col] = header_names.index(col)
            except ValueError:
                print(
                    f"  Could not find required column '{col}' in ESPN table headers: {header_names}"
                )
                return None

        data_rows_found = 0
        for row in stats_table.find_all("tr"):
            # Skip header rows or any other non-data rows
            if row.find("th") or "colhead" in row.get(
                "class", []
            ):  # get("class", []) handles no class attr
                continue

            cells = row.find_all("td")
            # Ensure row has enough cells for the columns we need
            if len(cells) <= max(
                col_indices.values()
            ):  # Use <= because indices are 0-based
                # print(f"  Skipping row with insufficient cells: {[c.get_text(strip=True) for c in cells]}")
                continue

            try:
                rk_val = cells[col_indices["RK"]].get_text(strip=True)
                player_name = cells[col_indices["PLAYER"]].get_text(strip=True)
                pts_str = (
                    cells[col_indices["PTS"]].get_text(strip=True).replace(",", "")
                )

                # Attempt to convert points to int, handle non-numeric gracefully
                try:
                    pts_val = int(pts_str)
                except ValueError:
                    # print(f"  Could not convert PTS '{pts_str}' to int for player {player_name}. Storing as string.")
                    pts_val = pts_str  # Keep as string if not convertible, or could be None/NaN

                scraped_data.append(
                    {"RK": rk_val, "Player": player_name, "PTS": pts_val}
                )
                data_rows_found += 1
            except IndexError:
                # print(f"  IndexError while processing a row. Cells: {[c.get_text(strip=True) for c in cells]}")
                continue  # Skip this malformed row
            except Exception as e:
                # print(f"  Unexpected error processing row: {e}. Row: {[c.get_text(strip=True) for c in cells]}")
                continue  # Skip this problematic row

        if not scraped_data:
            print("  No data rows were successfully scraped from the ESPN table.")
            return None

        print(f"  Successfully scraped {data_rows_found} player rows from ESPN.")
        return pd.DataFrame(scraped_data)

    except requests.exceptions.RequestException as e:
        print(f"  Error fetching ESPN page {url}: {e}")
        return None
    except Exception as e:
        print(f"  An unexpected error occurred during ESPN scraping: {e}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    csv_filename = (
        "espn_nba_leaders_pts_standalone.csv"  # Different name for standalone test
    )
    df_espn_leaders = scrape_espn_nba_leaders()

    if df_espn_leaders is not None and not df_espn_leaders.empty:
        print("\nESPN Scraped Data (first 5 rows):")
        print(df_espn_leaders.head().to_string())
        try:
            df_espn_leaders.to_csv(csv_filename, index=False)
            print(f"\nESPN data saved to {csv_filename}")
        except Exception as e:
            print(f"\nError saving ESPN data to CSV: {e}")
    else:
        print("No data was scraped from ESPN or an error occurred.")
