# Databricks notebook source
import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def find_region_latlong(region_list):
    geolocator = Nominatim(user_agent='myapplication')
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    geocodes = [{region : geocode(region)} for region in region_list]
    return geocodes

def pull_weather_data(latitude, longitude):
	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://api.open-meteo.com/v1/forecast"
	params = {
		"latitude": latitude,
		"longitude": longitude,
		"daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "daylight_duration", "uv_index_max", "rain_sum", "showers_sum", "snowfall_sum"],
		"timezone": "GMT",
		"start_date": "2024-03-01",
		"end_date": "2024-03-21"
	}
	records = []
	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	
	for idx, response in enumerate(responses):
		# Process daily data. The order of variables needs to be the same as requested.
		daily = response.Daily()
		daily_weather_code = daily.Variables(0).ValuesAsNumpy()
		daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
		daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
		daily_daylight_duration = daily.Variables(3).ValuesAsNumpy()
		daily_uv_index_max = daily.Variables(4).ValuesAsNumpy()
		daily_rain_sum = daily.Variables(5).ValuesAsNumpy()
		daily_showers_sum = daily.Variables(6).ValuesAsNumpy()
		daily_snowfall_sum = daily.Variables(7).ValuesAsNumpy()

		daily_data = {"date": pd.date_range(
			start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
			end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
			freq = pd.Timedelta(seconds = daily.Interval()),
			inclusive = "left"
		)}

		daily_data["latitude"] = latitude[idx]
		daily_data["longitude"] = longitude[idx]
		daily_data["elevation"] = response.Elevation()
		daily_data["weather_code"] = daily_weather_code
		daily_data["temperature_2m_max"] = daily_temperature_2m_max
		daily_data["temperature_2m_min"] = daily_temperature_2m_min
		daily_data["daylight_duration"] = daily_daylight_duration
		daily_data["uv_index_max"] = daily_uv_index_max
		daily_data["rain_sum"] = daily_rain_sum
		daily_data["showers_sum"] = daily_showers_sum
		daily_data["snowfall_sum"] = daily_snowfall_sum
		records.append(daily_data)
	
	return records
