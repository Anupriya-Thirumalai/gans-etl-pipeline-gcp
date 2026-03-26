
# Gans E-Scooter ETL Pipeline (GCP)

An automated ETL pipeline built for Gans, a fictional e-scooter startup, as part of a data engineering project. The pipeline collects city, weather, and flight data from external sources, transforms it with Python and pandas, and loads it into a relational MySQL database. The pipeline was deployed to Google Cloud Platform (GCP) and scheduled to run automatically once per day.

A full write-up of this project is available on Medium: [How I Built It (Step by Step)](https://medium.com/@anupriya.thirumalai/how-i-built-it-step-by-step-9e5cdd5dc022)

---

## Project Context

This project was completed as a case study during a Data Science Bootcamp (WBS Coding school, Berlin, 2025-2026), simulating a real-world data engineering scenario.

E-scooter companies face a core operational challenge: scooters end up unevenly distributed across a city due to asymmetric usage patterns (hilly terrain, commuter flows, rain, tourist activity). Gans wants to anticipate scooter demand by collecting external data that correlates with usage. This pipeline is the first step: building a reliable, automated data collection and storage system.

---

## Pipeline Design

The pipeline is split into two layers:

**Static layer** (run once locally): data that rarely changes, collected and stored as a foundation.
- City names, coordinates, and country scraped from Wikipedia
- Population data scraped from Wikipedia and stored per city
- Nearby airport metadata fetched from the AeroDataBox API using city coordinates

**Dynamic layer** (automated in the cloud): data that changes daily and needs to be refreshed on a schedule.
- Weather forecasts for each city fetched from the OpenWeatherMap API
- Flight arrivals for each airport fetched from the AeroDataBox API

---

## Database Schema

The MySQL database (`gans_local`) contains five tables with foreign key relationships:

```
city_info         — city name, country, latitude, longitude
city_population   — population per city per date
weather_data      — 5-day weather forecast per city (updated daily)
airport_info      — airport metadata linked to each city
flights_info      — flight arrivals per airport (updated daily)
```

The full schema is in `gans_local_db_creation.sql`.

---

## Implementation

### Local Pipeline (`etl_pipeline.py`)

Four functions handle data extraction, transformation, and loading:

`fetch_cities()` — scrapes city coordinates, country, and population from Wikipedia using BeautifulSoup. Cleans and loads into `city_info` and `city_population` tables.

`fetch_weather()` — reads city coordinates from the database, calls the OpenWeatherMap forecast API for each city, parses JSON, and loads 5-day forecast data into `weather_data`.

`fetch_airports()` — uses city coordinates to call the AeroDataBox API and retrieve nearby airport codes and metadata. Loads into `airport_info`.

`fetch_flights(icao_list)` — for each airport ICAO code, makes two 12-hour API calls (morning and afternoon) to retrieve next-day arrivals. Parses and loads into `flights_info`.

All credentials (database host, user, password, API keys) are managed via a `.env` file and loaded with `python-dotenv`.

### Cloud Deployment (GCP)

After validating the local pipeline, the dynamic functions (`fetch_weather` and `fetch_flights`) were deployed as Google Cloud Functions connected to a Cloud SQL MySQL instance. Cloud Scheduler was configured to trigger both functions once per day, making the pipeline fully automated. The pipeline ran successfully for several days before the GCP account was decommissioned at the end of the project.

---

## Repository Structure

```
gans-etl-pipeline-gcp/
├── etl_pipeline.py              # ETL script (local pipeline)
├── gans_local_db_creation.sql   # MySQL schema creation script
├── requirements.txt             # Python dependencies
├── .gitignore                   # excludes .env and sensitive files
└── README.md
```

---

## Setup and Usage

1. Clone the repository
2. Create a `.env` file in the root directory with the following variables:

```
host=your_mysql_host
user=your_mysql_user
password=your_mysql_password
port=3306
OPENWEATHER_API_KEY=your_openweather_key
RAPIDAPI_KEY=your_rapidapi_key
```

3. Set up the database by either:
- Running the SQL file from terminal:
```bash
mysql -u your_user -p < gans_local_db_creation.sql
```
- Or opening `gans_local_db_creation.sql` directly in MySQL Workbench and executing it

4. Run the pipeline:
```bash
python etl_pipeline.py
```

---

## Dependencies

- beautifulsoup4
- requests
- pandas
- python-dotenv
- pytz
- lat-lon-parser
- pymysql
- sqlalchemy

---

## Author

Anupriya Thirumalai
PhD in Neuroscience, University of Göttingen
[LinkedIn](https://www.linkedin.com/in/anupriyathirumalai/) | [ORCID](https://orcid.org/0000-0002-3624-0343) | [Medium](https://medium.com/@anupriya.thirumalai)