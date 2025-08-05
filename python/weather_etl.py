import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from geopy.geocoders import Nominatim
import sqlite3

def get_coordinates(city_name):
    """
    Get the latitude and longitude of a given city name using the Nominatim service.

    Parameters:
    city_name (str): The name of the city to geolocate.

    Returns:
    list: A list containing [latitude, longitude] if found, otherwise None.
    """
    geolocator = Nominatim(user_agent="get_coordinates")
    location = geolocator.geocode(city_name)
    
    if location is None:
        raise ValueError(f"Could not find location for city: {city_name}")

    return [location.latitude, location.longitude]

def get_weather_data(latitude, longitude):
    """
    Retrieve weather data from the Open-Meteo API for a specific location.

    Parameters:
    latitude (float): Latitude of the location.
    longitude (float): Longitude of the location.

    Returns:
    dict: A dictionary containing the API response data.
    """
    
    # Validate latitude and longitude
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        raise ValueError("Latitude must be between -90 and 90 and Longitude between -180 and 180.")

    # Setup the Open-Meteo API client with caching and retry mechanisms
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Prepare the API request parameters
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": ["temperature_2m", "relative_humidity_2m", 
                    "apparent_temperature", "is_day", "precipitation", "rain", 
                    "showers", "snowfall", "weather_code", "cloud_cover", 
                    "pressure_msl", "surface_pressure", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"],
        "timezone": "Europe/London",
        "forecast_days": 1
    }
    responses = openmeteo.weather_api(url, params=params)
    
    # Assuming single response (for one location)
    response = responses[0]

    return response

def extract_data(city_name):
    """
    Extract data from the API by first getting coordinates and then retrieving weather data.
    """
    coordinates = get_coordinates(city_name)
    raw_data = get_weather_data(coordinates[0], coordinates[1])
    return raw_data, coordinates

def transform_data(response, coordinates):
    """
    Format the weather data response into a structured Pandas DataFrame.

    Parameters:
    response (dict): The raw response from the Open-Meteo API.

    Returns:
    pd.DataFrame: A DataFrame containing the formatted weather data.
    """

    # Extract daily data from the API response
    current = response.Current()

    current_data = {}
    
    current_data["latitude"] = coordinates[0]
    current_data["longitude"] = coordinates[1]

    current_data["current_temperature_2m"] = current.Variables(0).Value()
    current_data["current_relative_humidity_2m"] = current.Variables(1).Value()
    current_data["current_apparent_temperature"] = current.Variables(2).Value()
    current_data["current_is_day"] = current.Variables(3).Value()
    current_data["current_precipitation"] = current.Variables(4).Value()
    current_data["current_rain"] = current.Variables(5).Value()
    current_data["current_showers"] = current.Variables(6).Value()
    current_data["current_snowfall"] = current.Variables(7).Value()
    current_data["current_weather_code"] = current.Variables(8).Value()
    current_data["current_cloud_cover"] = current.Variables(9).Value()
    current_data["current_pressure_msl"] = current.Variables(10).Value()
    current_data["current_surface_pressure"] = current.Variables(11).Value()
    current_data["current_wind_speed_10m"] = current.Variables(12).Value()
    current_data["current_wind_direction_10m"] = current.Variables(13).Value()
    current_data["current_wind_gusts_10m"] = current.Variables(14).Value()

    return current_data

def create_database(db_name="weather_data.db"):
    """
    Create a SQLite database and define the table schema.

    Parameters:
    db_name (str): The name of the SQLite database file.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Define the table schema
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            city_name TEXT,
            latitude REAL,
            longitude REAL,
            current_temperature_2m REAL,
            current_relative_humidity_2m REAL,
            current_apparent_temperature REAL,
            current_is_day INTEGER,
            current_precipitation REAL,
            current_rain REAL,
            current_showers REAL,
            current_snowfall REAL,
            current_weather_code INTEGER,
            current_cloud_cover REAL,
            current_pressure_msl REAL,
            current_surface_pressure REAL,
            current_wind_speed_10m REAL,
            current_wind_direction_10m REAL,
            current_wind_gusts_10m REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

def load_data_to_db(city_name, data, db_name="weather_data.db"):
    """
    Load the transformed data into the SQLite database.

    Parameters:
    city_name (str): The name of the city for which the data is being loaded.
    data (dict): The transformed weather data.
    db_name (str): The name of the SQLite database file.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Insert data into the weather table
    cursor.execute('''
        INSERT INTO weather (city_name, latitude, longitude, 
                             current_temperature_2m, current_relative_humidity_2m, 
                             current_apparent_temperature, current_is_day, current_precipitation, 
                             current_rain, current_showers, current_snowfall, 
                             current_weather_code, current_cloud_cover, 
                             current_pressure_msl, current_surface_pressure, 
                             current_wind_speed_10m, current_wind_direction_10m, 
                             current_wind_gusts_10m)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        city_name,
        data.get("latitude"),
        data.get("longitude"),
        data.get("current_temperature_2m"),
        data.get("current_relative_humidity_2m"),
        data.get("current_apparent_temperature"),
        data.get("current_is_day"),
        data.get("current_precipitation"),
        data.get("current_rain"),
        data.get("current_showers"),
        data.get("current_snowfall"),
        data.get("current_weather_code"),
        data.get("current_cloud_cover"),
        data.get("current_pressure_msl"),
        data.get("current_surface_pressure"),
        data.get("current_wind_speed_10m"),
        data.get("current_wind_direction_10m"),
        data.get("current_wind_gusts_10m"),
    ))

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

def run_etl_for_cities(cities, db_name="weather_data.db"):
    """
    Run the ETL pipeline for a list of cities and store the data in a SQLite database.

    Parameters:
    cities (list): List of city names to process.
    db_name (str): The name of the SQLite database file.
    """
    create_database(db_name)
    
    for city in cities:
        try:
            raw_data, coordinates = extract_data(city)
            transformed_data = transform_data(raw_data, coordinates )
            load_data_to_db(city, transformed_data, db_name)
            print(f"Successfully processed data for {city}.")
        except Exception as e:
            print(f"Failed to process data for {city}: {e}")

major_cities_by_region = [
    "Paris", "Lyon", "Marseille", "Lille", "Bordeaux",
    "Toulouse", "Nice", "Nantes", "Strasbourg", "Rennes", "Biarritz"
]


# Example usage:
run_etl_for_cities(major_cities_by_region)