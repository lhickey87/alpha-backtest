import lzma
import dill as pickle
from abc import abstractmethod
from datetime import datetime
import pandas as pd
import numpy as np


def load_pickle(file_path: str):
    with lzma.open(file_path, "wb") as fp:
        file = pickle.load(fp)
    return file

def save_pickle(file_path, object):
    with lzma.open(file_path, "wb") as fp:
        pickle.dump(object,fp)


def get_pnl(prev_weight, prev_units, prev_close, portfolio_idx, ret_row, portfolio) -> None:
    ret_row = np.nan_to_num(ret_row,nan=0,posinf=0,neginf=0)
    day_pnl = np.sum(prev_units*prev_close*ret_row)
    nominal_ret = np.dot(prev_weight,ret_row)
    capital_ret = nominal_ret * portfolio[portfolio_idx - 1, "leverage"]
    portfolio.at[portfolio_idx,"capital"] = portfolio.at[portfolio_idx - 1,"capital"] + day_pnl
    portfolio.at[portfolio_idx,"nominal_ret"] = nominal_ret
    portfolio.at[portfolio_idx,"day_pnl"] = day_pnl
    portfolio.at[portfolio_idx,"capital_ret"] = capital_ret
    return

class Alpha():

    def __init__(self,
                tickers: list[str],
                tickers_df: dict[str,pd.DataFrame],
                start: datetime,
                end: datetime,
                capital) -> None:

        self.tickers = tickers
        self.dfs = tickers_df
        self.start = start
        self.end = end
        self.startingCapital = capital
        self.portfolioVolume = 0.2
    #each day we will calculate pnl stats

    @abstractmethod
    def pre_compute(self, date_range: pd.Series):
        pass

    @abstractmethod
    def post_compute(self, date_range: pd.Series):
        pass

    @abstractmethod
    def compute_signals(self, eligibles, date):
        pass

    def is_any(x):
        return int(np.any(x))

    def compute_ticker_features(self, ticker: str):
        df = self.dfs[ticker]

        rets = df["close"].pct_change()
        vol = rets.rolling(30).std().ffill().fillna(0)

        sampled = df["close"] != df["close"].shift(1).bfill()
        eligible = sampled.rolling(5).apply(is_any,raw = True).fillna(0)

        return df["close"], rets, vol, eligible.astype(int)


    def tickers_meta_data(self, date_range: pd.Series):
        self.pre_compute(date_range)
        #now need to calc returns

        closes, eligibles, vols, returns = [], [], [], []
        for ticker in self.tickers:
            close, ret, vol, eligible = self.compute_ticker_features(ticker)
            closes.append(close)
            eligibles.append(eligible)
            returns.append(ret)
            vols.append(vol)

        self.closeDf = pd.concat(closes,axis=1)
        self.closeDf.columns = self.tickers
        self.volsDf = pd.concat(vols,axis=1)
        self.volsDf.columns = self.tickers
        self.retsDf = pd.concat(returns,axis=1)
        self.retsDf.columns = self.tickers
        self.eligiblesDf = pd.concat(eligibles,axis=1)
        self.eligiblesDf.columns = self.tickers

        self.post_compute(date_range)
        return

    def initializePortfolio(self, date_range: pd.Series):
        portfolio = pd.DataFrame(index = date_range)\
                    .reset_index()\
                    .rename(columns = {"index": "datetime"})
        portfolio.at[0,"capital"] = self.startingCapital
        portfolio.at[0,"day_pnl"] = 0
        portfolio.at[0,"capital_ret"] = 0
        portfolio.at[0,"nominal_ret"] = 0
        return portfolio

    def row_generator(self):
        for (portfolio_idx, portfolio_row),\
            (close_idx, close_row), \
            (ret_idx, ret_row), \
            (volatility_idx, volatility_row), \
            (eligibility_idx, eligibles_row) in zip(
                self.portfolio.iterrows(),
                self.closeDf.iterrows(),
                self.volsDf.iterrows(),
                self.retsDf.iterrows(),
                self.eligiblesDf.iterrows()
            ):
            yield {
                "portfolio_i": portfolio_idx,
                "portfolio_row": portfolio_row,
                "ret_i": ret_idx,
                "ret_row": ret_row,
                "close_row": close_row,
                "eligibles_row": eligibles_row,
                "vol_row": volatility_row,
                }


    def backtest(self):
        date_range = pd.date_range(self.start,self.end, freq = "D")
        self.portfolio = self.initializePortfolio(date_range)
        self.tickers_meta_data(date_range)
        #need to call the zipper function
        units, weights = [],[]
        prev_close = None
        for data in self.row_generator():
            portfolio_idx = data["portfolio_i"]
            portfolio_row = data["portfolio_row"]
            date = portfolio_row.loc["datetime"]
            ret_i = data["ret_i"]
            ret_row = data["ret_row"]
            close_row = data["close_row"]
            eligibles_row = data["eligibles_row"]
            vol_row = data["vol_row"]

            #so now that we have all of these we are able to call_pnl
            if portfolio_idx != 0:
                prev_units = units[-1]
                prev_weight = weights[-1]
                get_pnl(prev_weight = prev_weight,
                        prev_units = prev_units,
                        prev_close = prev_close,
                        portfolio_idx = portfolio_idx,
                        ret_row = ret_row,
                        portfolio = self.portfolio)

            # now we have alphas we can get the portfolio weight

            prev_close = close_row

            vol_target = (self.portfolio_vol / np.sqrt(253)) \
                            * self.portfolio.at[portfolio_idx,"capital"]
            #we have some set volatility target
            # weighting is based on capital_day * (daily_vol)
            weights = self.compute_signals(eligibles = eligibles_row,date = date)
