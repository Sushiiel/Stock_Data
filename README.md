# Real-Time Stock Data Extraction Pipeline

## Overview
This project is focused on extracting real-time stock data from 420+ Indian companies through web scraping. The extracted data is stored for further processing and analysis. The technologies used include:

- **Scrapy** for efficient web scraping.
- **BeautifulSoup** for parsing HTML content.
- **No Selenium** is used to avoid performance overhead.
- **Moneycontrol** is the primary data source.

## Data Source
The stock data is extracted in real-time from [Moneycontrol](https://www.moneycontrol.com/), focusing on 420+ Indian companies.

## Pipeline Architecture
1. **Extract**: Scrape stock data from Moneycontrol.
2. **Transform**: Clean and preprocess the extracted data.
3. **Load**: Store processed data for analysis and visualization.

## Files in this Repository
- `scraper.py`: Web scraper using Scrapy and BeautifulSoup.
- `stocks_data.csv`: Extracted stock data.
- `processing.py`: Data transformation and cleaning script.

## Setup & Execution
### Prerequisites
- Python 3.x.
- Required Python libraries (`scrapy`, `beautifulsoup4`, `pandas`).

### Steps
1. Clone this repository:
   ```sh
   git clone https://github.com/Sushiiel/Stock_Data.git
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Run the scraper:
   ```sh
   python scraper.py
   ```
4. Process and clean the extracted data:
   ```sh
   python processing.py
   ```

- **Your Name** - [Your GitHub Profile](https://github.com/Sushiiel)
