from py_etl.stock_pipeline.extract import extract_trades
from py_etl.stock_pipeline.transform import transform_trades
from py_etl.stock_pipeline.load import load_trades
from helpers.weather_pipeline_helpers.helpers import create_pg_engine, build_trade_model
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, text
from sqlalchemy.dialects import postgresql
from pathlib import Path 
import pandas as pd
import json
import yaml


class TestTrades:

    def setup_class(self):

        config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.json"

        # data ingestion config 
        data_config = r"/Users/kingmoh/Desktop/first-etl/secrets/config.yaml"

        with open(config_path) as config_file:
            self.config = json.load(config_file)

        with open(data_config) as data_config:
            self.data_config = yaml.safe_load(data_config)





    def test_extract_trades(self):

        stock_ticker = 'tsla'
        start_date = '2022-01-01'
        end_date = '2022-01-10'

        date_range = [{"start_date": start_date, "end_date": end_date}]

        # Get some trades for tsla 
        data = extract_trades(stock_ticker, date_range)

        assert isinstance(data, list) == True

        assert len(data) > 0



    def test_transform_trades(self):

        stock_ticker, start_date, end_date = 'tsla', '2022-01-01', '2022-01-03'

        # get some trades 
        trades = extract_trades(stock_ticker, [{"start_date": start_date, "end_date": end_date}])


        # transform the data 
        trades_df = transform_trades(trades)

        assert isinstance(trades_df, pd.DataFrame)




    def test_load(self):

        stock_ticker, start_date, end_date = 'tsla', '2022-01-01', '2022-01-03'


        # get some trades 
        trades = extract_trades(stock_ticker, [{"start_date": start_date, "end_date": end_date}])


        # transform the trades 
        trades_df = transform_trades(trades)


        # load the trades
        load_trades(trades_df, target_table_name="tbl_test_load_trades")




class TestModels:



    def test_build_trades_model(self):

        model = f"./models/stock_pipeline/staging_trades.sql"
        target_table = "test_model"
        engine = create_pg_engine()


        # read in the exchange codes
        exchange_codes = pd.read_csv('./data/stock_pipeline/exchange_codes.csv')
        exchange_codes_mod = {record[0]: record[1] for record in exchange_codes.to_dict(orient='tight').get('data')}


        # build the model
        try:

            build_trade_model(model, target_table, engine, text, exchange_codes_mod)

        except BaseException as err:
            pytest.fail(f"Unexpected error: {err}")




