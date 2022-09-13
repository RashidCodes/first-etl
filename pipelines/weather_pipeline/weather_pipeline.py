from py_etl.weather_pipeline_etl.extract import extract_weather 
from py_etl.weather_pipeline_etl.transform import transform_weather 
from py_etl.weather_pipeline_etl.load import load_weather
from helpers.weather_pipeline_helpers.helpers import create_pg_engine
import logging 



def weather_pipeline():

    log_file = f"./logs/weather_pipeline_logs/weather_pipeline.log"


    # Logging
    # format: https://docs.python.org/3/library/logging.html#logging.LogRecord
    logging.basicConfig(format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s", filename=log_file) 
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)


    cities = ["canberra", "sydney", "darwin", "brisbane", "adelaide"] 


    weather_data = [] 

    logger.info("Commencing extract")


    for city_name in cities:

        data = extract_weather(city_name) # return json

        # persist the result 
        weather_data.append(data)


    logger.info("Extract complete") 



    # convert to dataframe 
    logger.info("Commencing weather data transformation")
    weather_data = transform_weather(weather_data)   # returns pd.DataFrame

    logger.info("Transformation complete")


    logger.info("Commencing loading")
    if load_weather(weather_data):
        logger.info("Data load complete")
        exit()

    logger.error("ETL process failed")




# extract some data 
if __name__ == "__main__":
    weather_pipeline()
