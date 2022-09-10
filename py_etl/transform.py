import pandas as pd 
import json 


def transform_weather(weather_data: json):

    # normalise the data 
    df_weather_city = pd.json_normalize(weather_data)


    # add some more columns 
    df_weather_city["city_name"] = df_weather_city["name"].str.lower()


    return df_weather_city
    
