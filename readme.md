# Basketball Data Pipeline

This project implements an end-to-end data pipeline using Dagster to scrape basketball player data from Basketball Reference and ESPN, clean and merge the data, and save it to CSV files.

## Project Overview

This pipeline extracts basketball player data from two complementary sources:
- **Basketball Reference**: Detailed player profiles (personal info, physical attributes, career data)
- **ESPN**: NBA scoring leaders and rankings

The data is then cleaned, standardized, and merged using fuzzy name matching to create a comprehensive dataset combining biographical information with performance statistics.

## Project Structure

```
dagster-semi/
├── semis_pipeline/
│   ├── pipeline.py          # Main Dagster pipeline definition
│   ├── processing/
│   │   ├── __init__.py
│   │   └── cleaning.py      # Data cleaning and merging logic
│   └── scrapers/
│       ├── __init__.py
│       ├── basketball_reference.py  # BR scraper
│       └── espn.py         # ESPN scraper
├── setup.py                # Automated setup and runner script
├── dagster.yaml            # Dagster configuration
├── workspace.yaml          # Workspace configuration
└── README.md               # This file
```

## Features

### Data Sources
1. **Basketball Reference**: 
   - Player names, nicknames, pronunciation
   - Birth date and location
   - Physical attributes (height, weight)
   - Career information (position, college, draft year, NBA debut)
   - Respects 3-second delay between requests

2. **ESPN**: 
   - Current NBA scoring leaders
   - Player rankings and points scored
   - Single-page extraction for efficiency

### Pipeline Components

1. **Web Scraping Assets**:
   - `basketball_reference_data`: Scrapes up to 100 players per letter (A-Z) (Limited due to huge data size but can be adjusted)
   - `espn_leaders_data`: Scrapes current NBA scoring leaders

2. **Data Processing Assets**:
   - `cleaned_merged_data`: Advanced data cleaning with:
     - Name normalization and fuzzy matching
     - Date standardization (multiple formats → YYYY-MM-DD)
     - Height standardization (various formats → feet-inches)
     - Location formatting

3. **Data Quality Checks**:
   - **Basketball Reference**: Completeness validation (≥80% name coverage)
   - **ESPN**: Scoring data validity checks (≥90% numeric values)
   - **Merged Data**: Integration quality assessment

4. **Scheduling Options**:
   - Daily updates (2 AM)
   - Weekly updates (Sunday 3 AM)
   - Manual execution via web interface

## Quick Start

### Prerequisites
- Python 3.8+
- Internet connection

### Setup & Run

1. **Setup everything**:
   ```bash
   python setup.py setup
   ```

2. **Start Dagster**:
   ```bash
   python setup.py dev
   ```

3. **Run the pipeline**:
   - Open http://localhost:3000
   - Click **"Assets"** → **"Materialize All"**
   - Wait for the pipeline to finish

4. **Check results**:
   ```bash
   python setup.py check
   ```

### Alternative: Run Once via CLI
```bash
python setup.py run
```

**That's it!** Your basketball data will be in the `data_outputs/` folder.

## Usage Guide

### Using the Web Interface

1. **Navigate to Assets**: View the pipeline's data assets
2. **Monitor Progress**: Real-time execution tracking with detailed logs
3. **Check Quality**: Built-in data validation results
4. **Download Results**: Access output files directly

### Expected Execution Time

- **ESPN Scraping**: ~30 seconds (single page)
- **Basketball Reference**: ~2hrs (100 players per letter)
- **Data Processing**: ~1-2 minutes (cleaning + merging)

### Command Line Options

```bash
# Run specific assets
dagster asset materialize -f semis_pipeline/pipeline.py -d semis_pipeline --select basketball_reference_data

# Execute complete job
dagster job execute -f semis_pipeline/pipeline.py -d semis_pipeline -j basketball_pipeline_job

# View pipeline structure
dagster asset list -f semis_pipeline/pipeline.py -d semis_pipeline
```

## Output Files

Generated in `data_outputs/` directory:

1. **`basketball_reference_players.csv`**
   - Raw player data from Basketball Reference
   - ~2,600 rows (100 per letter A-Z)
   - 17 columns including biographical and career data

2. **`espn_nba_leaders_pts.csv`**
   - Current NBA scoring leaders
   - Ranking, player name, and points scored

3. **`merged_players_data.csv`** 
   - **Primary output**: Cleaned and merged dataset
   - Combined BR biographical data + ESPN performance data
   - Fuzzy name matching for accurate player association
   - Standardized data formats

### Sample Merged Data Structure
```csv
ESPN_Rank,Name,ESPN_Points,Position,Height_Imperial,Born_Date,Born_Location,NBA_Debut,College,...
1,LeBron James,37000,Forward,6-9,1984-12-30,"Akron, Ohio",2003-10-29,None,...
```

## Configuration

Customize pipeline behavior in `semis_pipeline/pipeline.py`:

```python
class DataPipelineConfig(Config):
    max_players_per_letter: int = 100    # Limit per letter (A-Z)
    espn_url: str = "https://www.espn.com/nba/history/leaders"
    output_dir: str = "data_outputs"
    # Custom filenames for each output...
```

## Data Quality Features

### Automated Validation
- **Completeness**: Ensures essential fields are populated
- **Format Validation**: Checks data types and value ranges  
- **Integration Success**: Verifies merge accuracy and match rates

### Fuzzy Name Matching
Advanced algorithm handles name variations:
- "Shaquille O'Neal" ↔ "shaquille oneal"
- Case normalization and punctuation removal
- Configurable similarity threshold (default: 80%)

### Data Standardization
- **Dates**: Multiple formats → YYYY-MM-DD
- **Heights**: Various formats → feet-inches (e.g., "6-9")
- **Names**: Normalized for consistent matching
- **Locations**: Cleaned formatting with consistent punctuation

## Scheduling

Configure automated runs in the Dagster web interface:

- **Daily Schedule**: 2 AM daily (for updated ESPN rankings)
- **Weekly Schedule**: Sunday 3 AM (for comprehensive updates)
- **Manual Triggers**: On-demand execution anytime

## Web Scraping Ethics

The pipeline follows responsible scraping practices:

✅ **Respects robots.txt**: Manually verified compliance  
✅ **Rate limiting**: 3-second delays between Basketball Reference requests  
✅ **Appropriate headers**: Realistic User-Agent strings  
✅ **Public data only**: No authentication required  
✅ **Error handling**: Graceful failure management  
✅ **Timeout management**: 15-second request timeouts  

## License

Educational use only. Respects all website terms of service and robots.txt guidelines.

---

**Need help?** Check the Dagster logs in the web interface or review the console output for detailed error messages.