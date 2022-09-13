from pandas import DataFrame


def transform_trades(trades: list):

    """ 
    Transfrom the trades returned from Alpaca 

    
    Parameters
    ----------
    trades: list 
        The trades


    Returns
    -------
    trades_df: DataFrame 
        Transformed trades


    """

    trades_df = DataFrame({
        "tradeID": [trade.get("i") for trade in trades],
        "timestamp": [trade.get("t") for trade in trades],
        "exchange": [trade.get("x") for trade in trades],
        "price": [trade.get("p") for trade in trades],
        "size": [trade.get("s") for trade in trades] 
    })

    return trades_df
