import pandas as pd 
import json 
from py_etl.weather_pipeline_etl.extract import extract_weather
from py_etl.weather_pipeline_etl.transform import transform_weather
from py_etl.weather_pipeline_etl.load import load_weather
from helpers.weather_pipeline_helpers.helpers import create_pg_engine



class TestWeather:

    def setup_class(self):
        
        config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.json"

        with open(config_path) as config_file:
            self.config = json.load(config_file)




    def test_extract_weather(self):

        # Get the weather information for canberra 
        data = extract_weather("canberra")

        assert len(data) > 0




    def test_transform_weather(self):

        # Transform the data returned from open weather 
        data = extract_weather("canberra") 


        # returns a pandas dataframe 
        transformed_weather = transform_weather(data)

        # check out the shape 
        assert isinstance(transformed_weather, pd.DataFrame) == True 
        assert transformed_weather.shape[0] > 0




    def test_load_weather(self):

        status = False

        # get data 
        data = extract_weather("canberra") # returns json

        # transform data 
        transformed_weather = transform_weather(data)

        # load data 
        engine = create_pg_engine()

        try:

            # try loading the data
            load_weather(transformed_weather)

        except BaseException as err:
            print(err)
            pass

        else:
            status = True 


        assert status == True














