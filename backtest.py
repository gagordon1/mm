#!/usr/bin/env python3
"""
backtest.py – Backtesting framework with pluggable strategies (using State and Trade types from strategies.py)
"""
import argparse
import importlib
from pathlib import Path
from typing import List, Dict, Callable, Optional, TypedDict

import pandas as pd

# Type definition for the state of a single exchange's order book level for a given trading pair
Ticker = TypedDict('Ticker', {
    'bid':       Optional[float],
    'ask':       Optional[float],
    'bid_size':  Optional[float],
    'ask_size':  Optional[float],
    'funding_rate': Optional[float]
})

# Type alias for Positions structure
Positions = Dict[str, Dict[str, float]] # {venue: {coin: balance}}

# --- More descriptive type aliases for Ticker structure --- 
PairTickerMap = Dict[str, Ticker]           # Maps pair string to Ticker data
VenueTickerMap = Dict[str, PairTickerMap]   # Maps venue string to PairTickerMap
# --- --- --- 

# Type definition for a single trade leg in an arbitrage strategy
Trade = TypedDict('Trade', {
    'pair':      str,
    'venue':     str,
    'side':      str,  # 'buy' or 'sell'
    'price':     float,
    'volume':    float,
    'fee':       float,
    'ts_ns': Optional[float]
})

class State:
    """
    State class representing the current state of the market and positions.
    """
    def __init__(self, tickers: VenueTickerMap, positions: Positions):
        self.tickers = tickers
        self.positions = positions

    def __str__(self) -> str:
        """
        Format the state as a readable string for debugging.
        """
        lines = []
        lines.append("State:")
        
        # Format positions
        lines.append("  Positions:")
        for venue, assets in self.positions.items():
            lines.append(f"    {venue}:")
            for asset, balance in assets.items():
                lines.append(f"      {asset}: {balance:,.8f}")
        
        # Format tickers
        lines.append("  Tickers:")
        for venue, pairs in self.tickers.items():
            lines.append(f"    {venue}:")
            for pair, ticker in pairs.items():
                bid = ticker.get('bid', 'None')
                ask = ticker.get('ask', 'None')
                bid_sz = ticker.get('bid_size', 'None')
                ask_sz = ticker.get('ask_size', 'None')
                funding = ticker.get('funding_rate', 'None')
                lines.append(f"      {pair}:")
                lines.append(f"        bid: {bid:,.8f} x {bid_sz:,.8f}")
                lines.append(f"        ask: {ask:,.8f} x {ask_sz:,.8f}")
                if funding != 'None':
                    lines.append(f"        funding: {funding:,.8f}")
        
        return "\n".join(lines)


def load_parquet_directory(path: Path) -> pd.DataFrame:
    """
    Load all Parquet files under `path`, concatenate, sort by ts_ns, and return.
    Expects columns: ts_ns, venue, pair, bid, ask, bid_size, ask_size, [funding_rate].
    """
    files = list(path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files in {path}")
    df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    df = df.sort_values("ts_ns").reset_index(drop=True)
    return df


class Backtester:
    def __init__(
        self,
        data: pd.DataFrame,
        initial_positions: Dict[str, Dict[str, float]],
        strategy: Callable[[State], List[Trade]]
    ):
        """
        :param data: historical DataFrame with ts_ns and BBO columns
        :param initial_positions: {{venue: {{asset: balance}}}}
        :param strategy: function(state) -> List[Trade]
        """
        self.data = data.copy()
        self.positions = {v: assets.copy() for v, assets in initial_positions.items()}
        self.strategy = strategy
        self.trades: List[Trade] = []
        # initialize state using the exact State TypedDict
        self.state: State = State(tickers={}, positions=self.positions)

    def _update_book(self, rec: pd.Series) -> None:
        venue = rec["venue"]
        pair = rec["pair"]
        # Ticker dict matches strategies.py Ticker TypedDict
        ticker: Ticker = {
            "bid":          rec["bid"],
            "ask":          rec["ask"],
            "bid_size":     rec.get("bid_size"),
            "ask_size":     rec.get("ask_size"),
            "funding_rate": rec.get("funding_rate")
        }
        self.state.tickers.setdefault(venue, {})[pair] = ticker

    def _execute_trade(self, t: Trade) -> None:
        venue = t["venue"]
        pair  = t["pair"]
        side  = t["side"]
        price = t["price"]
        vol   = t["volume"]
        fee   = t["fee"]
        base, quote = pair.split("/")

        # ensure positions entry exists
        bal = self.positions.setdefault(venue, {})
        bal.setdefault(base,  0.0)
        bal.setdefault(quote, 0.0)

        if side == "buy":
            bal[quote] -= price * vol + fee
            bal[base]  += vol
        else:  # sell
            bal[base]  -= vol
            bal[quote] += price * vol - fee

        # record executed trade leg
        self.trades.append(t)

    def run(self) -> pd.DataFrame:
        """
        Iterate over each row in historical data, update orderbook,
        invoke strategy, and execute any returned trades.
        Returns a DataFrame of all executed Trade records.
        """
        for _, row in self.data.iterrows():
            # update book state for this tick
            self._update_book(row)
            # call strategy with current state
            new_trades = self.strategy(self.state)
            for t in new_trades:
                # stamp execution time
                t["ts_ns"] = row["ts_ns"]
                self._execute_trade(t)
        # return trades log as DataFrame
        return pd.DataFrame(self.trades)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run backtest on historical Parquet files.")
    parser.add_argument(
        "--path", required=True,
        help="Directory containing Parquet files"
    )
    parser.add_argument(
        "--strategy", required=True,
        help="Name of strategy function in strategies.py"
    )
    parser.add_argument(
        "--initial", nargs=2, action="append", metavar=("VENUE","ASSETS"),
        help="Initial positions as VENUE 'asset:amt,asset:amt'", default=[]
    )
    args = parser.parse_args()

    # load historical data
    df = load_parquet_directory(Path(f"data/{args.path}"))

    # parse initial positions
    initial = {}
    for venue, asset_str in args.initial:
        d = {}
        for pair in asset_str.split(','):
            token, amt = pair.split(':')
            d[token] = float(amt)
        initial[venue] = d

    # dynamic strategy import
    mod = importlib.import_module('strategies')
    if not hasattr(mod, args.strategy):
        raise AttributeError(f"Strategy '{args.strategy}' not found")
    strat_fn = getattr(mod, args.strategy)

    # run backtest
    bt = Backtester(df, initial, strat_fn)
    results = bt.run()
    print(results)

