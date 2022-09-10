import datetime as dt 
import json
import requests

def extract_weather(city_name: str, units: str = "metric") -> list:


    # Configuration 
    config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.json"

    with open(config_path) as config_file:
        config = json.load(config_file)


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








def extract_trades(stock_ticker: str, date_range: "list(dict)") -> list:

    """ 
    Get trades for multiple date ranges using the alpaca API 


    Parameters
    ----------
    ticker: str 
        Ticker e.g. AAPL 

    date_range: A list of date ranges 
        e.g. [{'start_date': '01/01/2022', 'end_date': '02/01/2022'}, {}, ...]



    Returns
    -------
    response_data: A list of trades 

    """ 

    base_url = f"https://data.alpaca.markets/v2/stocks/{stock_ticker}/trades"

    dev_base_url = f"https://paper-api.alpaca.markets/v2/stocks/{stock_ticker}/trades"


    # Config information 
    config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.json"
    
    with open(config_path) as config_file:
        config = json.load(config_file)



    response_data = []

    for generated_date in date_range:

        start_time = generated_date.get("start_date")
        end_time = generated_date.get("end_date") 


        params = {"start": start_time, "end" : end_time}

        print(params)


        # Authentication 
        headers = {
               "APCA-API-KEY-ID" : config["alpaca"]["api_key_id"],
               "APCA-API-SECRET-KEY": config["alpaca"]["api_secret_key"]
        }


        response = requests.get(dev_base_url)

        print(f"Status Code: {response.status_code}")
        print(f"Response: {response}")


        if response.json().get("trades") is not None:
            response_data.extend(response.json().get("trades"))



    return response_data




