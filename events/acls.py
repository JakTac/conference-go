from .keys import PEXELS_API_KEY, OPEN_WEATHER_API_KEY
import requests


def get_photo(city, state):
    url = f"https://api.pexels.com/v1/search?query={city}+{state}"

    headerz = {"Authorization": PEXELS_API_KEY}

    resp = requests.get(url, headers=headerz)
    data = resp.json()
    return data["photos"][0]["src"]["original"]


def get_coordinates(city, state):
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},{state},US&limit=5&appid={OPEN_WEATHER_API_KEY}"
    resp = requests.get(url)
    return {
        "lat": resp.json()[0]["lat"],
        "lon": resp.json()[0]["lon"],
    }


def get_weather_data(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=imperial&appid={OPEN_WEATHER_API_KEY}"
    resp = requests.get(url)
    return {
        "description": resp.json()["weather"][0]["description"],
        "temp": resp.json()["main"]["temp"],
    }
