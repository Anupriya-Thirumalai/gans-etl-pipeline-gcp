"""
ETL pipeline for Gans e-scooter demand forecasting.
Collects city, weather, and flight data from external sources
and loads it into a MySQL database.

Static data (cities, populations, airports): run once locally.
Dynamic data (weather, flights): deployed as GCP Cloud Functions,
triggered daily by Cloud Scheduler.

Usage: python etl_pipeline.py
Requires: .env file with database credentials and API keys.
"""

from bs4 import BeautifulSoup
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import datetime as dt
from pytz import timezone
from lat_lon_parser import parse

def establish_sql_connection():
    # connecting to sql now and adding this there: os.getenv() and then parse(latitude)
  load_dotenv()
  schema = 'gans_local'
  host = os.getenv("host")
  user = os.getenv("user")
  password = os.getenv("password")
  port = int(os.getenv("port"))
  return f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'


def fetch_cities():
  """Scrapes city coordinates and population from Wikipedia and loads into city_info and city_population tables."""

  cities=['Berlin', 'Hamburg', 'Munich','Cologne','Frankfurt']
  city_info_df=[]; city_population_df=[]
  for c, city in enumerate(cities, start=1):
     url = f"https://en.wikipedia.org/wiki/{cities[c-1]}"
     headers = {'User-Agent': 'Chrome/134.0.0.0'}
     response = requests.get(url, headers=headers)
     soup_city = BeautifulSoup(response.content, 'html.parser')
     soup_city.find('title')
     coordinates = soup_city.find("table",class_="infobox ib-settlement vcard")
     latitude= coordinates.select('td span.latitude')[0].get_text()
     longitude= coordinates.select('span.longitude')[0].get_text()
     country=coordinates.find(string="Country").find_next().get_text()
    
     population=soup_city.find('table', class_="infobox ib-settlement vcard").find_all(string="Population")[0].find_next(class_='infobox-data').get_text()
     row_city_info_df = {'city_name': city,'country': country,'latitude': parse(latitude), 'longitude': parse(longitude)}
     city_info_df.append(row_city_info_df)
 
     today = dt.datetime.today().strftime("%d.%m.%Y")
     today = pd.to_datetime(today, format="%d.%m.%Y")
     row_city_population_df={'city_id': c,'timestamp_population':today,'population':population }
     city_population_df.append(row_city_population_df)
     # Convert list of dicts to a DataFrame
  city_info_df=pd.DataFrame(city_info_df)
  city_population_df=pd.DataFrame(city_population_df)
  city_population_df['population'] = city_population_df['population'].str.replace(',', '').astype(int)
  print(city_info_df)
  print(city_population_df)
  connection_string=establish_sql_connection()
  city_info_df.to_sql('city_info',
                  if_exists='append',
                  con=connection_string,
                  index=False)
  city_population_df.to_sql('city_population',
                  if_exists='append',
                  con=connection_string,
                  index=False)



def fetch_weather():
  """Fetches 5-day weather forecast for each city from OpenWeatherMap API and loads into weather_data table."""

  berlin_timezone = timezone('Europe/Berlin')
  connection_string= establish_sql_connection()
  city_info_df = pd.read_sql("SELECT * FROM city_info",con=connection_string)
  weather_data={'city_id':[],'forecast_time':[], 'outlook': [],
                 'temperature':[],'feels_like':[],'wind_speed':[],
                   	'rain_prob':[],'rain_in_last_3h':[],'data_retrieved_at':[]}
  
  retrieval_time = dt.datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")
  for c in city_info_df['city_id']:
     lat= city_info_df['latitude'][c-1]
     lon=city_info_df['longitude'][c-1]
     API_key=os.getenv("OPENWEATHER_API_KEY")
     weather=requests.get(f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API_key}&units=metric")
     weather_json=weather.json()
  
     for ll in range (len(weather_json['list'])):    
       weather_data['city_id'].append(city_info_df['city_id'][c-1])
       weather_data['forecast_time'].append(weather_json['list'][ll]['dt_txt'])
       weather_data['outlook'].append(weather_json['list'][ll]['weather'][0]['description'])
       weather_data['temperature'].append(weather_json['list'][ll]['main']['temp'])
       weather_data['feels_like'].append(weather_json['list'][ll]['main']['feels_like'])
       weather_data['wind_speed'].append(weather_json['list'][ll]['wind']['speed'])
       weather_data['rain_prob'].append(weather_json['list'][ll]['pop'])
       weather_data["rain_in_last_3h"].append(weather_json['list'][ll].get("rain", {}).get("3h", 0))
       weather_data["data_retrieved_at"].append(retrieval_time)
  weather_data_df=pd.DataFrame(weather_data) 
  print(weather_data_df)
  weather_data_df.to_sql('weather_data',
                  if_exists='append',
                  con=connection_string,
                  index=False)
  


def fetch_airports():
  """Fetches nearby airport metadata for each city using AeroDataBox API and loads into airport_info table."""

  connection_string= establish_sql_connection()
  city_info_df = pd.read_sql("SELECT * FROM city_info",con=connection_string)

  url = "https://aerodatabox.p.rapidapi.com/airports/search/location"
  airports_list_df={'city_id':[],'icao_info':[],'iata_info':[],
    'airport_name':[],'airport_municipality':[],'airport_timezone':[] }

  for c in city_info_df['city_id']:  
     querystring = {"lat":city_info_df['latitude'][c-1],
                   "lon":city_info_df['longitude'][c-1],
                   "radiusKm":"50","limit":"10",
                   "withFlightInfoOnly":"true"}
     headers = {
      	"x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": "aerodatabox.p.rapidapi.com"}  
     response = requests.get(url, headers=headers, params=querystring)
     print(response.json())
     resp_json=response.json()
     for airp in range (len(resp_json['items'])):
       airports_list_df['city_id'].append(c)       
       airports_list_df['icao_info'].append(resp_json['items'][airp].get('icao'))
       airports_list_df['iata_info'].append(resp_json['items'][airp].get('iata'))
       airports_list_df['airport_name'].append(resp_json['items'][airp].get('name'))
       airports_list_df['airport_municipality'].append(resp_json['items'][airp].get('municipalityName'))
       airports_list_df['airport_timezone'].append(resp_json['items'][airp].get('timeZone'))
  airports_list_df=pd.DataFrame(airports_list_df) 
  airports_list_df.to_sql('airport_info',
                  if_exists='append',
                  con=connection_string,
                  index=False)
  return airports_list_df
  


def fetch_flights(icao_list):
  """Fetches next-day flight arrivals for each airport ICAO code and loads into flights_info table."""
  
  berlin_timezone = timezone('Europe/Berlin')
  today = dt.datetime.now(berlin_timezone).date()
  tomorrow = (today + dt.timedelta(days=1))
  tomorrow = tomorrow.strftime('%Y-%m-%d')
  print(tomorrow)
  flight_items = []

  for icao in icao_list:
    # the api can only make 12 hour calls, therefore, two 12 hour calls make a full day
    # using the nested lists below we can make a morning call and extract the data
    # then make an afternoon call and extract the data
    times = [["00:00","11:59"],
             ["12:00","23:59"]]

    for time in times:
      url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/EDDF/{tomorrow}T{time[0]}/{tomorrow}T{time[1]}"

      querystring = {"withLeg":"true","direction":"Arrival",
               "withCancelled":"false","withCodeshared":"true",
               "withCargo":"false","withPrivate":"false","withLocation":"false"}

      headers = {
	  "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
	  "x-rapidapi-host": "aerodatabox.p.rapidapi.com"}


      response = requests.get(url, headers=headers, params=querystring)
      print(response)
      flights_json = response.json()

      retrieval_time = dt.datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")

      for item in flights_json["arrivals"]:
        flight_item = {
            "arrival_airport_icao": icao,
            "departure_airport_icao": item["departure"]["airport"].get("icao", None),
            "scheduled_arrival_time": item["arrival"]["scheduledTime"].get("local", None),
            "flight_number": item.get("number", None),
            "data_retrieved_at": retrieval_time
        }
        flight_items.append(flight_item)

  flights_df = pd.DataFrame(flight_items)
  flights_df["scheduled_arrival_time"] = flights_df["scheduled_arrival_time"].str[:-6]
  flights_df["scheduled_arrival_time"] = pd.to_datetime(flights_df["scheduled_arrival_time"])
  flights_df["data_retrieved_at"] = pd.to_datetime(flights_df["data_retrieved_at"])
  connection_string=establish_sql_connection()
  flights_df.to_sql('flights_info',
                  if_exists='append',
                  con=connection_string,
                  index=False)
  return flights_df




if __name__=="__main__":
  fetch_cities()
  fetch_weather()
  airports_list_df= fetch_airports()
  flights_df=fetch_flights(airports_list_df['icao_info'].to_list())

