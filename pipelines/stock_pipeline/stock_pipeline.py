from py_etl.stock_pipeline.extract import extract_trades
from pathlib import Path
import json
import yaml




if __name__ == '__main__':


    config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.json"
    pipeline_conf = r"/Users/kingmoh/Desktop/first-etl/pipeline/stock_pipeline/config.yaml"

    file = (f"{Path.cwd()}/pipelines/stock_pipeline/config.yaml")


    # secrets config
    with open(config_path) as config_file:
        config = json.load(config_file)

    # data ingestion config
    with open(file) as pipeline_config:
        pip_conf = yaml.safe_load(pipeline_config)




    # Extract some data

    stocks = pip_conf.get("stocks")

    for stock_details in stocks:

        stock = stock_details.get("stock").get("extract")

        ticker = stock.get("stock_tickers")
        date_picker = stock.get("date_picker")
        start_date = stock.get("start_date")
        end_date = stock.get("end_date")


        # extract the trades 
        trades = extract_trades(ticker, [{"start_date": start_date, "end_date": end_date}])


        print(trades)

        exit()


