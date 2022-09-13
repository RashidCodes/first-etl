from py_etl.stock_pipeline.extract import extract_trades
from helpers.weather_pipeline_helpers.helpers import create_pg_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from sqlalchemy.dialects import postgresql
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



    def test_load(self):

        stock_ticker, start_date, end_date = 'tsla', '2022-01-01', '2022-01-03'

        # get some trades 
        trades = extract_trades(stock_ticker, [{"start_date": start_date, "end_date": end_date}])


        # create a dataframe of the trades 
        trades_df = pd.DataFrame({
            "tradeID": [trade.get("i") for trade in trades],
            "timestamp": [trade.get("t") for trade in trades], 
            "exchange": [trade.get("x") for trade in trades], 
            "price": [trade.get("p") for trade in trades], 
            "size": [trade.get("s") for trade in trades]

        })

        print(trades_df.head())


        target_table_name = "tbl_test_load_trades"
        target_database_engine = create_pg_engine()

        # create the target table if it does not exist 
        meta = MetaData()

        stock_price_test_trades_table = Table(
                target_table_name, meta,
                Column("tradeID", String, primary_key=True),
                Column("timestamp", String, primary_key=True),
                Column("exchange", String, primary_key=True),
                Column("price", Float),
                Column("size", Integer)
        )

        # create table if it doesn't exist 
        meta.create_all(target_database_engine)

        insert_statement = postgresql.insert(stock_price_test_trades_table).values(trades_df.to_dict(orient="records"))

        upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=['tradeID', 'timestamp', 'exchange'],
                set_={c.key: c for c in insert_statement.excluded if c.key not in ['tradeID', 'timestamp', 'exchange']} 
        )

        target_database_engine.execute(upsert_statement)
                



