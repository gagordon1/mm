#!/usr/bin/env python3
"""
backtest.py – Backtesting framework with pluggable strategies (using State and Trade types)
Tracks PnL including funding income and fees.
"""
import argparse
import importlib
from pathlib import Path
from typing import List, Dict, Callable, Optional, TypedDict, Any

import pandas as pd

# Type definitions
Ticker = TypedDict('Ticker', {
    'bid': Optional[float], 'ask': Optional[float],
    'bid_size': Optional[float], 'ask_size': Optional[float],
    'funding_rate': Optional[float]
})
Positions = Dict[str, Dict[str, float]]  # {venue: {asset: balance}}
Trade = TypedDict('Trade', {
    'pair': str, 'venue': str, 'side': str,
    'price': float, 'volume': float, 'fee': float,
    'ts_ns': Optional[float], 'type': str  # 'spot','perp'
})

Event = TypedDict('Event', {
    'ts_ns': Optional[float],
    'type': str,  # 'trade', 'funding'
    'data': Dict[str, Any]  # trade data or funding data
})

class State:
    def __init__(self, tickers: Dict[str, Dict[str, Ticker]], positions: Positions):
        self.tickers = tickers
        self.positions = positions

    def __str__(self) -> str:
        lines = ['State:']
        lines.append('  Positions:')
        for v, assets in self.positions.items():
            lines.append(f'    {v}: {assets}')
        lines.append('  Tickers:')
        for v, pairs in self.tickers.items():
            lines.append(f'    {v}:')
            for p, t in pairs.items():
                lines.append(
                    f'      {p} | bid={t.get("bid")} x{t.get("bid_size")} ; '
                    f'ask={t.get("ask")} x{t.get("ask_size")} ; '
                    f'funding={t.get("funding_rate")}')
        return "\n".join(lines)

def load_parquet_directory(path: Path) -> pd.DataFrame:
    files = list(path.glob('*.parquet'))
    if not files:
        raise FileNotFoundError(f'No Parquet files in {path}')
    df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    df = df.sort_values('ts_ns').reset_index(drop=True)
    return df

class Backtester:
    def __init__(
        self,
        data: pd.DataFrame,
        initial_positions: Positions,
        strategy: Callable[[State], List[Trade]]
    ):
        self.data = data.copy()
        self.positions = {v: assets.copy() for v, assets in initial_positions.items()}
        self.strategy = strategy
        self.events: List[Event] = []
        self.state = State(tickers={}, positions=self.positions)
        # funding schedule: hourly
        self.funding_interval_ns = int(3600 * 1e9)
        first_ts = self.data['ts_ns'].iloc[0]
        self.next_funding_ts = ((first_ts // self.funding_interval_ns) + 1) * self.funding_interval_ns

    def _update_book(self, rec: pd.Series) -> None:
        venue, pair = rec['venue'], rec['pair']
        ticker: Ticker = {
            'bid': rec['bid'], 'ask': rec['ask'],
            'bid_size': rec.get('bid_size'), 'ask_size': rec.get('ask_size'),
            'funding_rate': rec.get('funding_rate')
        }
        self.state.tickers.setdefault(venue, {})[pair] = ticker

    def _execute_trade(self, t: Trade) -> None:
        venue, pair = t['venue'], t['pair']
        side, price, vol, fee = t['side'], t['price'], t['volume'], t['fee']
        acct = self.positions.setdefault(venue, {})
        if t['type'] == 'spot':
            base, quote = pair.split('/')
        else:  # perp
            base, quote = pair, 'USDC'
        acct.setdefault(base, 0.0); acct.setdefault(quote, 0.0)
        if side == 'buy':
            acct[quote] -= price * vol + fee
            acct[base]  += vol
        else:
            acct[base]  -= vol
            acct[quote] += price * vol - fee
        self.events.append(Event({
            'ts_ns': t['ts_ns'],
            'type': 'trade',
            'data': dict(t)  # Convert Trade to dict
        }))

    def _accrue_funding(self) -> None:
        # apply funding to USDC balances at scheduled times
        for venue, assets in self.positions.items():
            for pair, pos_qty in list(assets.items()):
                if not pair.endswith('-PERP') or pos_qty == 0:
                    continue
                tick = self.state.tickers.get(venue, {}).get(pair, {})
                rate = tick.get('funding_rate') or 0.0
                price = tick.get('bid') if pos_qty < 0 else tick.get('ask')
                if price is None:
                    continue
                pnl = -pos_qty * price * rate
                assets['USDC'] = assets.get('USDC', 0.0) + pnl
                # Record funding as an event
                self.events.append(Event({
                    'ts_ns': self.next_funding_ts,
                    'type': 'funding',
                    'data': {
                        'venue': venue,
                        'pair': pair,
                        'position': pos_qty,
                        'rate': rate,
                        'price': price,
                        'pnl': pnl
                    }
                }))

    def run(self) -> pd.DataFrame:
        initial_usdc = sum(acct.get('USDC', 0.0) for acct in self.positions.values())
        events = []
        
        for _, row in self.data.iterrows():
            # accrue any due funding
            while row['ts_ns'] >= self.next_funding_ts:
                self._accrue_funding()
                self.next_funding_ts += self.funding_interval_ns
            # update orderbook and apply strategy
            self._update_book(row)
            new_trades = self.strategy(self.state)
            for t in new_trades:
                t['ts_ns'] = row['ts_ns']
                self._execute_trade(t)
                # Record USDC balance after each event
                current_usdc = sum(acct.get('USDC', 0.0) for acct in self.positions.values())
                events.append({
                    'ts_ns': row['ts_ns'],
                    'type': 'trade',
                    'usdc_balance': current_usdc,
                    'pnl': current_usdc - initial_usdc
                })
        
        # Convert to DataFrame
        df = pd.DataFrame(events)
        if not df.empty:
            df = df.sort_values('ts_ns')
        return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run backtest')
    parser.add_argument('--path', required=True)
    parser.add_argument('--strategy', required=True)
    parser.add_argument('--initial', nargs=2, action='append', default=[])
    args = parser.parse_args()

    data = load_parquet_directory(Path(f"data/{args.path}"))
    initial = {}
    for venue, s in args.initial:
        parts = s.split(',')
        balances = {token: float(amt) for token, amt in (p.split(':') for p in parts)}
        initial[venue] = balances
    mod = importlib.import_module('strategies')
    strat = getattr(mod, args.strategy)
    bt = Backtester(data, initial, strat)
    result = bt.run()
    print(result)
