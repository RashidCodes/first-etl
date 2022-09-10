from py_etl.extract import extract_weather 
from py_etl.transform import transform_weather 
from py_etl.load import load_weather
from helpers.helpers import create_pg_engine



def weather_pipeline():

    cities = ["canberra", "sydney", "darwin", "brisbane", "adelaide"] 


    weather_data = [] 


    for city_name in cities:

        data = extract_weather(city_name) # return json

        # persist the result 
        weather_data.append(data)



    # load it
    # convert to dataframe 
    weather_data = transform_weather(weather_data)   # returns pd.DataFrame


    if load_weather(weather_data):
        print("Successfully loaded weather data")
        exit()


    print("ETL process failed")



# extract some data 
if __name__ == "__main__":
    weather_pipeline()
