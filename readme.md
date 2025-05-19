# Data Warehousing SemiFinal Project - Webscraping with Dagster
### Project Overview
This project demonstrates a web scraping and data integration pipeline developed using **Dagster**, a modern data orchestrator designed for machine learning, analytics, and ETL workflows.

The pipeline collects and processes data from **two reputable basketball statistics websites**:  
- [**Basketball Reference**](https://www.basketball-reference.com/players/): for detailed player information.  
- [**ESPN**](https://www.espn.com/nba/history/leaders): for historical NBA points leaders.

### Workflow Highlights  
1. **Web Scraping**  
   - Extracts the top NBA points leaders from ESPN.  
   - For each player, navigates to their corresponding page on Basketball Reference to gather additional statistics and bio information.

2. **Data Cleaning & Transformation**  
   - Raw HTML data is parsed and processed using **BeautifulSoup** and **Pandas**.  
   - Irrelevant or duplicate fields are removed, and names are standardized to enable accurate joining.

3. **Data Integration**  
   - The datasets from both sources are merged into a unified **Pandas DataFrame** based on player names and identifiers.

4. **Storage**  
   - The final cleaned dataset is exported and saved as a **CSV file**, ready for analysis or further ETL processing.
