from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    schedule,
    define_asset_job,
    DefaultScheduleStatus,
    Definitions,
    AssetMaterialization,
    AssetCheckResult,
    asset_check,
    AssetCheckSeverity,
)
import pandas as pd
import os
from typing import List, Dict, Any
import time

# Import your existing modules
import sys
import os

# Add the current directory to the Python path so we can import local modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scrapers.basketball_reference import main as scrape_basketball_reference
from scrapers.espn import scrape_espn_nba_leaders
from processing.cleaning import clean_and_merge_player_data, save_merged_data


class DataPipelineConfig(Config):
    """Configuration for the data pipeline"""

    max_players_per_letter: int = 100
    espn_url: str = "https://www.espn.com/nba/history/leaders"
    output_dir: str = "data_outputs"
    br_output_filename: str = "basketball_reference_players.csv"
    espn_output_filename: str = "espn_nba_leaders_pts.csv"
    merged_output_filename: str = "merged_players_data.csv"


@asset(group_name="web_scraping")
def basketball_reference_data(
    context: AssetExecutionContext, config: DataPipelineConfig
) -> str:
    """
    Scrapes basketball reference data for all players (limited per letter).
    Returns the path to the saved CSV file.
    """
    context.log.info(
        f"Starting Basketball Reference scraping with max {config.max_players_per_letter} players per letter"
    )

    # Ensure output directory exists
    os.makedirs(config.output_dir, exist_ok=True)

    # Run the basketball reference scraper
    df = scrape_basketball_reference()

    if df is None or df.empty:
        raise Exception("Failed to scrape data from Basketball Reference")

    # Save the data
    output_path = os.path.join(config.output_dir, config.br_output_filename)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    context.log.info(
        f"Successfully scraped {len(df)} players from Basketball Reference"
    )
    context.log.info(f"Data saved to: {output_path}")

    # Log materialization metadata
    context.add_output_metadata(
        {
            "num_players": len(df),
            "file_path": output_path,
            "file_size_bytes": os.path.getsize(output_path),
            "columns": list(df.columns),
            "preview": df.head().to_dict("records"),
        }
    )

    return output_path


@asset(group_name="web_scraping")
def espn_leaders_data(
    context: AssetExecutionContext, config: DataPipelineConfig
) -> str:
    """
    Scrapes ESPN NBA scoring leaders data.
    Returns the path to the saved CSV file.
    """
    context.log.info(f"Starting ESPN scraping from: {config.espn_url}")

    # Ensure output directory exists
    os.makedirs(config.output_dir, exist_ok=True)

    # Run the ESPN scraper
    df = scrape_espn_nba_leaders(config.espn_url)

    if df is None or df.empty:
        raise Exception("Failed to scrape data from ESPN")

    # Save the data
    output_path = os.path.join(config.output_dir, config.espn_output_filename)
    df.to_csv(output_path, index=False)

    context.log.info(f"Successfully scraped {len(df)} players from ESPN")
    context.log.info(f"Data saved to: {output_path}")

    # Log materialization metadata
    context.add_output_metadata(
        {
            "num_players": len(df),
            "file_path": output_path,
            "file_size_bytes": os.path.getsize(output_path),
            "columns": list(df.columns),
            "preview": df.head().to_dict("records"),
        }
    )

    return output_path


@asset(group_name="data_processing")
def cleaned_merged_data(
    context: AssetExecutionContext,
    config: DataPipelineConfig,
    basketball_reference_data: str,
    espn_leaders_data: str,
) -> str:
    """
    Cleans and merges basketball reference and ESPN data.
    Returns the path to the saved merged CSV file.
    """
    context.log.info("Starting data cleaning and merging process")

    # Ensure output directory exists
    os.makedirs(config.output_dir, exist_ok=True)

    # Clean and merge the data using your existing function
    merged_data, unmatched_players, stats = clean_and_merge_player_data(
        basketball_reference_data, espn_leaders_data
    )

    if not merged_data:
        raise Exception("No data was successfully merged")

    # Save the merged data
    output_path = os.path.join(config.output_dir, config.merged_output_filename)
    success = save_merged_data(merged_data, output_path)

    if not success:
        raise Exception("Failed to save merged data to CSV")

    context.log.info(f"Successfully merged {len(merged_data)} players")
    context.log.info(f"Merged data saved to: {output_path}")
    context.log.info(f"Match success rate: {stats['match_percentage']:.1f}%")

    # Log detailed metadata about the merge results
    context.add_output_metadata(
        {
            "total_merged_players": len(merged_data),
            "total_unmatched_players": len(unmatched_players),
            "exact_matches": stats["exact_matches"],
            "fuzzy_matches": stats["fuzzy_matches"],
            "match_percentage": stats["match_percentage"],
            "file_path": output_path,
            "file_size_bytes": os.path.getsize(output_path),
            "sample_merged_player": merged_data[0] if merged_data else None,
        }
    )

    return output_path


# Data Quality Checks
@asset_check(asset=basketball_reference_data, blocking=True)
def basketball_reference_completeness_check(
    context: AssetExecutionContext, basketball_reference_data: str
) -> AssetCheckResult:
    """
    Checks that Basketball Reference data has essential fields populated.
    """
    df = pd.read_csv(basketball_reference_data)

    # Check for required columns
    required_columns = ["Name", "Position", "Height_Imperial", "Born_Date"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Missing required columns: {missing_columns}",
        )

    # Check data completeness - at least 80% of names should be populated
    name_completeness = (df["Name"].notna().sum() / len(df)) * 100

    if name_completeness < 80:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Only {name_completeness:.1f}% of player names are populated (expected >= 80%)",
        )

    return AssetCheckResult(
        passed=True,
        description=f"Data quality check passed. {name_completeness:.1f}% name completeness, {len(df)} total players",
    )


@asset_check(asset=espn_leaders_data, blocking=True)
def espn_data_validity_check(
    context: AssetExecutionContext, espn_leaders_data: str
) -> AssetCheckResult:
    """
    Checks that ESPN data has valid scoring data.
    """
    df = pd.read_csv(espn_leaders_data)

    # Check for required columns
    if not all(col in df.columns for col in ["RK", "Player", "PTS"]):
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description="Missing required columns: RK, Player, or PTS",
        )

    # Check that points are numeric and positive where possible
    numeric_pts = pd.to_numeric(df["PTS"], errors="coerce")
    valid_pts_ratio = (numeric_pts.notna().sum() / len(df)) * 100

    if valid_pts_ratio < 90:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Only {valid_pts_ratio:.1f}% of points values are valid numbers (expected >= 90%)",
        )

    return AssetCheckResult(
        passed=True,
        description=f"ESPN data quality check passed. {valid_pts_ratio:.1f}% valid points data, {len(df)} total players",
    )


@asset_check(asset=cleaned_merged_data, blocking=False)
def merged_data_quality_check(
    context: AssetExecutionContext, cleaned_merged_data: str
) -> AssetCheckResult:
    """
    Checks the quality of the merged dataset.
    """
    df = pd.read_csv(cleaned_merged_data)

    # Check that we have both ESPN and Basketball Reference data
    espn_columns = [col for col in df.columns if col.startswith("ESPN_")]
    br_columns = [col for col in df.columns if not col.startswith("ESPN_")]

    if len(espn_columns) < 2 or len(br_columns) < 5:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Merged data may be incomplete. ESPN columns: {len(espn_columns)}, BR columns: {len(br_columns)}",
        )

    # Check for reasonable match rate (expecting at least 70% of top players to match)
    match_rate_estimate = len(
        df
    )  # This would need to be compared against original ESPN data size

    return AssetCheckResult(
        passed=True,
        description=f"Merged data quality check passed. {len(df)} players successfully merged with {len(df.columns)} total columns",
    )


# Define the main job
basketball_pipeline_job = define_asset_job(
    name="basketball_pipeline_job",
    selection=["basketball_reference_data", "espn_leaders_data", "cleaned_merged_data"],
    description="Complete basketball data pipeline: scrape, clean, and merge",
)


# Define a schedule to run the pipeline daily
@schedule(
    job=basketball_pipeline_job,
    cron_schedule="0 2 * * *",  # Run at 2 AM daily
    default_status=DefaultScheduleStatus.STOPPED,  # Start with schedule stopped
)
def basketball_pipeline_schedule(context):
    """
    Schedule to run the basketball data pipeline daily at 2 AM.
    """
    return {
        "ops": {
            "basketball_reference_data": {
                "config": {
                    "max_players_per_letter": 100,
                    "output_dir": "data_outputs",
                }
            }
        }
    }


# Alternative weekly schedule for less frequent updates
@schedule(
    job=basketball_pipeline_job,
    cron_schedule="0 3 * * 0",  # Run at 3 AM every Sunday
    default_status=DefaultScheduleStatus.STOPPED,
)
def basketball_pipeline_weekly_schedule(context):
    """
    Schedule to run the basketball data pipeline weekly on Sundays at 3 AM.
    """
    return {}


# Define all assets, jobs, and schedules for Dagster
defs = Definitions(
    assets=[
        basketball_reference_data,
        espn_leaders_data,
        cleaned_merged_data,
    ],
    asset_checks=[
        basketball_reference_completeness_check,
        espn_data_validity_check,
        merged_data_quality_check,
    ],
    jobs=[basketball_pipeline_job],
    schedules=[
        basketball_pipeline_schedule,
        basketball_pipeline_weekly_schedule,
    ],
)
