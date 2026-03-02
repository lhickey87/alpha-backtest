import threading
from datetime import datetime
from io import StringIO

import pandas as pd
import pytz
import requests
import yfinance as yf

url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
header = {"User-Agent": "Mozilla/5.0"}


def getSP_tickers():
    res = requests.get(url, headers=header)
    tables = pd.read_html(StringIO(res.text))

    sp500 = None
    for table in tables:
        if "Symbol" in table.columns:
            sp500 = table
            break

    if sp500 is None:
        raise ValueError("Could not Find table")
    # we should turn this column into a list and print it
    tickers = sp500["Symbol"].to_list()
    return tickers


def get_history(ticker, period_start, period_end, granularity="1d", tries=0):
    # we need to replace ticker
    try:
        df = (
            yf.Ticker(ticker)
            .history(
                start=period_start,
                end=period_end,
                interval=granularity,
                auto_adjust=True,
            )
            .reset_index()
        )
    except Exception as err:
        if tries < 5:
            return get_history(ticker, period_start, period_end, granularity, tries + 1)
        return pd.DataFrame()

    df = df.rename(
        columns={
            "Date": "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    if df.empty:
        return pd.DataFrame()

    df = df.drop(columns=["Dividends", "Stock Splits"])
    df = df.set_index("datetime", drop=True)
    return df


def get_histories(tickers, _start, _end, gran="1d", max_threads=12):
    if not (len(tickers) == len(_start) == len(_end)):
        raise ValueError("tickers, _start, and _end must have same length.")

    stockHistory = [None] * len(tickers)

    def _helper(i):
        df = get_history(tickers[i], _start[i], _end[i], granularity=gran)
        stockHistory[i] = df

    for start_i in range(0, len(tickers), max_threads):
        end_i = min(start_i + max_threads, len(tickers))
        batch = [
            threading.Thread(target=_helper, args=(i,)) for i in range(start_i, end_i)
        ]
        [thread.start() for thread in batch]
        [thread.join() for thread in batch]

    tickers = [tickers[i] for i in range(len(tickers)) if not stockHistory[i].empty]
    validStocks = [
        df for df in stockHistory if isinstance(df, pd.DataFrame) and not df.empty
    ]
    return tickers, validStocks


def get_ticker_dfs(start: datetime, end: datetime):
    tickers = getSP_tickers()
    tickers = [ticker.replace(".", "-") for ticker in tickers]
    tickers, stocks = get_histories(
        tickers, [start] * len(tickers), [end] * len(tickers)
    )
    # now it would be very nice to wrap it into a dictionary ticker as key value as dataframe
    tickers_df = {ticker: df for ticker, df in zip(tickers, stocks)}
    return tickers, tickers_df


# 1. Start the threads via [thread.start() for thread in threads]
# 2. Join them to relinquish control to caller thread [thread.join() for thread in threads]
# Now at this point we would have called the helper method on supposedely ALL of our tickers
# And so next we would need to look into what sorts of errors could arise from this call
# 1. Important to make sure that when we call the helper, we are able to do multiple tries??


start = datetime(2010, 1, 1, tzinfo=pytz.utc)
endTime = datetime.now(pytz.utc)
# so now we need to use multithreading in order to speed up process

tickers, tickersDf = get_ticker_dfs(start=start, end=endTime)

for ticker, dataframe in tickersDf.items():
    print()
    print(dataframe)
    input("Hello")

# so now that we actually have the tickers we should look to yfinance to download histories
