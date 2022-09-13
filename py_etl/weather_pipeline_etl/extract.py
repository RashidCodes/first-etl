import datetime as dt 
import json
import requests
import yaml



def extract_weather(city_name: str, units: str = "metric") -> list:


    # Configuration 

    config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.yaml"

    with open(config_path) as config_file:
        config = yaml.safe_load(config_file)



    params = {
            "q": city_name,
            "units": units,
            "appid": config["openWeather"]["api_key"]
    }

    # base url 
    base_url = f"http://api.openweathermap.org/data/2.5/weather"

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        return response.json()

    else:
        raise Exception("Extracting weather api data failed. Please check if API limits have been reached")
        return


