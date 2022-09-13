from py_etl.stock_pipeline.extract import extract_trades
import json


class TestTrades:

    def setup_class(self):

        config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.json"

        with open(config_path) as config_file:
            self.config = json.load(config_file)




    def test_extract_weather(self):

        stock_ticker = 'tsla'
        start_date = '2022-01-01'
        end_date = '2022-01-10'

        date_range = [{"start_date": start_date, "end_date": end_date}]

        # Get the weather information for canberra
        data = extract_trades(stock_ticker, date_range)

        assert len(data) > 0


