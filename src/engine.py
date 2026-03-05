import numpy as np
import pandas as pd
from datetime import datetime

def get_pnl(prev_weight, prev_units, prev_close, portfolio_idx, ret_row, portfolio) -> None:
    for ticker in tickers:
        units = portfolio.loc[idx - 1, f"{ticker} units"]
        if units != 0:
            delta = dfs[ticker].at[date,"close"] - dfs[ticker].at[prev_date,"close"]
            day_pnl += delta*units
            nominal_pnl += portfolio.loc[idx-1,f"{ticker} w"]*dfs[ticker].loc[date,"ret"]
    portfolio.loc[idx,"capital"] = portfolio.loc[idx - 1,"capital"] + day_pnl
    portfolio.loc[idx,"day_pnl"] = day_pnl
    portfolio.loc[idx,"nominal_returns"] = nominal_pnl
    portfolio.loc[idx,"capital_returns"] = nominal_pnl*portfolio.loc[idx-1,"leverage"]
    #walk through and actually compute pnl

class Portfolio:

    def __init__(self,
                tickers: list[str],
                tickers_df: dict[str,pd.DataFrame],
                start: datetime,
                end: datetime) -> None:

        self.tickers = tickers
        self.dfs = tickers_df
        self.start = start
        self.end = end
    #each day we will calculate pnl stats
    # pnl is based on the following
    # difference in pirce (today vs yesterday)
    # times our current position
    # additionally we have nominal and capital ret
    #
    def pre_compute(self,trade_range):
        for inst in self.tickers:
            inst_df = self.dfs[inst]
            alpha = -1 * (1-(inst_df.open/inst_df.close)).rolling(12).mean()
            self.dfs[inst]["alpha"] = alpha
        return

        # the reason we would
    def portfolio_df(self, date_range: pd.Series) -> pd.DataFrame:
        portfolio = (
            pd.DataFrame(index=date_range)
            .reset_index()
            .rename(columns={"index": "datetime"})
        )
        portfolio.loc[0, "capital"] = 10000
        portfolio.at[0,"day_pnl"] = 0.0
        portfolio.at[0,"capital_returns"] = 0.0
        portfolio.at[0,"nominal_returns"] = 0.0
        return portfolio

    def post_compute(self,trade_range):
        for inst in self.tickers:
            self.dfs[inst]["alpha"] = self.alphas[inst]
            self.dfs[inst]["alpha"] = self.dfs[inst]["alpha"].ffill()
            self.dfs[inst]["eligible"] = self.dfs[inst]["eligible"] \
                & (~pd.isna(self.dfs[inst]["alpha"]))
        return
    #this is only for a particular day
    # so question is about do we keep returns in self.dfs
    # or do we keep in portfolio
    # position determines units typically
    # leverage = (nominal amount of shares)/(capital in shares)
    #
        # pd.date_range(start,end) assumably provides a series
    def standardize_index(self, date_range) -> None:
        self.alphas = {}
        self.pre_compute(date_range)

        def is_any_one(x):
            return int(np.any(x))

        for inst in self.tickers:
            df = pd.DataFrame(index=date_range)
            inst_vol = (-1 + self.dfs[inst]["close"]/self.dfs[inst]["close"].shift(1)).rolling(30).std()
            self.dfs[inst] = df.join(self.dfs[inst]).ffill().bfill()
            self.dfs[inst]["ret"] = -1 + self.dfs[inst]["close"]/self.dfs[inst]["close"].shift(1)
            self.dfs[inst]["vol"] = inst_vol
            self.dfs[inst]["vol"] = self.dfs[inst]["vol"].ffill().fillna(0)
            self.dfs[inst]["vol"] = np.where(self.dfs[inst]["vol"] < 0.005, 0.005, self.dfs[inst]["vol"])
            sampled = self.dfs[inst]["close"] != self.dfs[inst]["close"].shift(1).bfill()
            eligible = sampled.rolling(5).apply(is_any_one,raw=True).fillna(0)
            self.dfs[inst]["eligible"] = eligible.astype(int) & (self.dfs[inst]["close"] > 0).astype(int)
            self.alphas[inst] = self.dfs[inst]["alpha"]

        self.post_compute(date_range)
        return

    def compute_signal(self,eligibles, date):
        forecasts = {}
        for inst in eligibles:
            forecasts[inst] = self.dfs[inst].at[date,"alpha"]
        return forecasts, np.sum(np.abs(list(forecasts.values())))

    def backtest(self):
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        self.standardize_index(date_range = date_range)
        portfolio = self.portfolio_df(date_range = date_range)

        for i in portfolio.index:
            date = portfolio.at[i,"datetime"]
            eligibles = [ticker for ticker in self.tickers if self.dfs[ticker].loc[date,"eligible"]]
            not_eligible = [ticker for ticker in self.tickers if ticker not in eligibles]

            if i!=0:
                date_prev = portfolio.at[i-1,"datetime"]
                get_pnl(portfolio = portfolio,
                        date = date,
                        prev_date = date_prev,
                        dfs=self.dfs,
                        idx = i,
                        tickers = eligibles)

            forecasts, total_chips = self.compute_signal(eligibles,date)
            vol_target = (0.2 / np.sqrt(253)) * portfolio.at[i,"capital"]

            for ticker in not_eligible:
                portfolio.loc[i,f"{ticker} units"] = 0
                portfolio.loc[i,f"{ticker} w"] = 0

            nominal_total = 0
            for inst in eligibles:
                forecast = forecasts[ticker]
                scaled_forecast = forecast / total_chips if total_chips != 0 else 0
                position = \
                    scaled_forecast \
                    * vol_target \
                    / (self.dfs[inst].at[date, "vol"] * self.dfs[inst].at[date,"close"])

                portfolio.at[i, inst + " units"] = position
                nominal_total += abs(position * self.dfs[inst].at[date,"close"])
                #now that we have units for the day we need to add to our nominal total

            for inst in eligibles:
                units = portfolio.at[i, inst + " units"]
                nominal_inst = units * self.dfs[inst].at[date,"close"]
                inst_w = nominal_inst / nominal_total
                portfolio.at[i, inst + " w"] = inst_w

            portfolio.at[i, "nominal"] = nominal_total
            portfolio.at[i, "leverage"] = nominal_total / portfolio.at[i, "capital"]

        return portfolio.set_index("datetime",drop=True)
