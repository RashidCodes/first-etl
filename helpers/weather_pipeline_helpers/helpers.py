import datetime as dt
import json
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql


def generate_datetime_ranges(
        start_date: str = None,
        end_date: str = None,
        days_from_start: int = None 
) -> list: 

    """ Generates a range of datetime dates.

    Parameters
    ----------
    start_date: str
        The start date with format "yyyy-mm-dd"

    end_date: str 
        The end date with format "yyyy-mm--dd" 



    Returns
    -------
    date_range: list 
        A list of dicts containing ranges 



    Example
    -------
    >> generate_datetime_ranges(start_date='2022-01-01', end_date='2022-01-03')

        [
            {'start_time': '2020-01-01T00:00:00.00Z', 'end_time': '2020-01-02T00:00:00.00Z'},
            {'start_time': '2020-01-02T00:00:00.00Z', 'end_time': '2020-01-03T00:00:00.00Z'}
        ]
    """

    start = dt.datetime.strptime(start_date, '%Y-%m-%d')


    if days_from_start is None:

        date_range = []
        
        end = dt.datetime.strptime(end_date, '%Y-%m-%d')


        while start < end:
            curr_start = start 
            new_start = curr_start + dt.timedelta(days=1)


            time_dict = {
                    'start_time': f"{dt.datetime.isoformat(curr_start)}Z",
                    'end_time': f"{dt.datetime.isoformat(new_start)}Z"
            }


            date_range.append(time_dict)

            start = start + dt.timedelta(days=1)


        return date_range



    return [{
        'start_time': f"{dt.datetime.isoformat(start)}Z", 
        'end_time': f"{dt.datetime.isoformat(start + dt.timedelta(days=days_from_start))}"

    }]





def create_pg_engine():

    """ Create engine to connect to Postgresql """

    
    # Config information 
    config_path = r"/Users/kingmoh/Desktop/first-etl/secrets/config.yaml"

    with open(config_path) as config_file:
        config = yaml.safe_load(config_file)

       
    # User info 
    db_user = config["database"]["db_user"]
    db_password = config["database"]["db_password"]
    db_server_name = config["database"]["db_server_name"]
    db_database_name = config["database"]["db_database_name"]
    db_port  = config["database"]["db_port"]


    connection_url = URL.create(
            drivername = "postgresql+pg8000",
            username = db_user,
            password = db_password,
            host = db_server_name,
            port = db_port,
            database = db_database_name
    )


    engine = create_engine(connection_url) 
    return engine 



