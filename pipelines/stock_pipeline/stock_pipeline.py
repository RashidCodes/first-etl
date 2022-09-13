from py_etl.stock_pipeline.extract import extract_trades
import json
import yaml







if __name__ == '__main__':


    config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.json"
    pipeline_conf = r"/Users/kingmoh/Desktop/first-etl/pipeline/stock_pipeline/config.yaml"

    with open(config_path) as config_file:
        config = json.load(config_file)


    with open(pipeline_conf) as pipeline_config:
        pip_conf = yaml.safe_load(pipeline_config)




    # Extract some data
    




    def test_extract_weather(self):

        stock_ticker = 'tsla'
        start_date = '2022-01-01'
        end_date = '2022-01-10'

        date_range = [{"start_date": start_date, "end_date": end_date}]

        # Get the weather information for canberra
        data = extract_trades(stock_ticker, date_range)

        assert len(data) > 0


