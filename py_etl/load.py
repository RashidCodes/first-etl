from helpers.helpers import create_pg_engine 
from py_etl.transform import transform_weather
import pandas as pd 



def load_weather(weather_data: pd.DataFrame):


    engine = create_pg_engine()

    try:
        weather_data.to_sql("tbl_weatherForecast", con=engine, if_exists='replace')

    except BaseException as err:
        print("An exception occurred")
        print (err)
        return False

    else:
        print("Success")
        return True



