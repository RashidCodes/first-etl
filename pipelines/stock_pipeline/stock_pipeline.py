from py_etl.stock_pipeline.extract import extract_trades
from py_etl.stock_pipeline.transform import transform_trades
from py_etl.stock_pipeline.load import load_trades
from helpers.weather_pipeline_helpers.helpers import generate_datetime_ranges
from pathlib import Path
from pandas import DataFrame, concat
import json
import yaml




if __name__ == '__main__':

    """ Extract trades from Alpaca and load them into Postgres """


    # An empty dataframe to store all trades from the pipeline config
    full_trades = DataFrame()


    config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.json"

    pipeline_conf = (f"{Path.cwd()}/pipelines/stock_pipeline/config.yaml")


    # secrets config
    with open(config_path) as config_file:
        config = json.load(config_file)


    # data ingestion config
    with open(pipeline_conf) as pipeline_config:
        pip_conf = yaml.safe_load(pipeline_config)




    # Extract some data
    stocks = pip_conf.get("stocks")

    for stock_details in stocks:

        stock = stock_details.get("stock").get("extract")

        ticker = stock.get("stock_tickers")
        date_picker = stock.get("date_picker")
        start_date = stock.get("start_date")
        end_date = stock.get("end_date")

        date_range = [{"start_date": start_date, "end_date": end_date}]

        
        if date_picker == "date_range":
            date_range = generate_datetime_ranges(start_date, end_date)


        # extract the trades 
        trades = extract_trades(ticker, date_range)

        # transform the trades 
        trades_df = transform_trades(trades)


        # push all the trades
        full_trades = concat([full_trades, trades_df])



    # load the trades 
    load_trades(full_trades, target_table_name="tbl_trades")

        

