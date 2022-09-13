from helpers.weather_pipeline_helpers.helpers import create_pg_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, Float 
from sqlalchemy.dialects import postgresql
from pandas import DataFrame



# load trades
def load_trades(df:DataFrame, target_table_name: str = "tbl_tesla_trades"):

    target_database_engine = create_pg_engine()

    # create table if it does not exist 
    meta = MetaData()

    stock_price_trades = Table(
            target_table_name, meta,
            Column("tradeID", String, primary_key=True),
            Column("timestamp", String, primary_key=True),
            Column("exchange", String, primary_key=True),
            Column("price", Float),
            Column("size", Integer) 
    )


    # create the table
    meta.create_all(target_database_engine)

    insert_statement = postgresql.insert(stock_price_trades).values(df.to_dict(orient="records"))

    upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=["tradeID", "timestamp", "exchange"],
            set_={c.key: c for c in insert_statement.excluded if c.key not in ['tradeID', 'timestamp', 'exchange']} 

    )


    target_database_engine.execute(upsert_statement)






