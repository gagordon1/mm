
#!/usr/bin/env python3
"""
backtest.py – Cross-exchange arbitrage backtester and PnL plotter (event-driven)

Reads per-exchange Parquet logs (with identical schema) for multiple venues,
builds a unified event stream of raw top-of-book updates, steps through
each tick updating the latest bid/ask for each exchange, and executes a
simple arbitrage strategy whenever a profitable opportunity arises.
Plots cumulative P&L at the end.

Usage:
    python backtest.py
"""
import pandas as pd
import glob
import matplotlib.pyplot as plt
from pathlib import Path

# ─────────── configuration ───────────
data_dir = Path('data')  # directory with per-exchange parquet files
EXCHANGES = ['binanceus', 'coinbase', 'hyperliquid', 'kraken']
SYMBOL    = 'BTC/USD'

# Maker fees per exchange (decimal fraction of notional per leg)
MAKER_FEES = {
    'binanceus':   0.000,
    'coinbase':    0.002,
    'hyperliquid': 0.00035,
    'kraken':      0.004,
}

# ─────────── build event stream ───────────
def build_event_df():
    events = []
    for ex in EXCHANGES:
        pattern = f"{data_dir}/{ex}_*.parquet"
        for path in glob.glob(pattern):
            df = pd.read_parquet(path)
            # filter to symbol
            df = df[df['pair'] == SYMBOL]
            if df.empty:
                continue
            # use timestamp index and keep raw columns
            df['ts'] = pd.to_datetime(df['ts_ns'], unit='ns')
            df['venue'] = ex
            events.append(df[['ts', 'ts_ns', 'pair', 'bid', 'ask', 'venue']])
    # concatenate all events and sort
    events_df = pd.concat(events, ignore_index=True)
    events_df = events_df.sort_values('ts').reset_index(drop=True)
    return events_df

# ─────────── backtest logic (event-driven) ───────────
def run_arbitrage(events_df):
    # initialize latest state per exchange
    state = {ex: {'bid': None, 'ask': None} for ex in EXCHANGES}
    records = []

    for _, row in events_df.iterrows():
        ex = row['venue']
        bid = row['bid']
        ask = row['ask']
        ts  = row['ts']
        # update only if changed
        updated = False
        if bid != state[ex]['bid']:
            state[ex]['bid'] = bid
            updated = True
        if ask != state[ex]['ask']:
            state[ex]['ask'] = ask
            updated = True
        # if nothing changed, skip
        if not updated:
            continue

        # find best arbitrage snapshot
        best = None
        best_pnl = 0.0
        for buy_ex in EXCHANGES:
            a = state[buy_ex]['ask']
            if a is None:
                continue
            for sell_ex in EXCHANGES:
                if sell_ex == buy_ex:
                    continue
                b = state[sell_ex]['bid']
                if b is None:
                    continue
                # compute PnL after fees
                fee = (MAKER_FEES[buy_ex] + MAKER_FEES[sell_ex]) * ((a + b) / 2)
                pnl = b - a - fee
                if pnl > best_pnl:
                    best_pnl = pnl
                    best = (buy_ex, sell_ex, a, b, fee)
        # record if profitable
        if best and best_pnl > 0:
            buy_ex, sell_ex, a, b, fee = best
            print(f"Buy on {buy_ex}, sell on {sell_ex}, ask {a}, bid {b}, fee {fee}, pnl {best_pnl}")
            records.append({
                'timestamp': ts,
                'buy_on':     buy_ex,
                'sell_on':    sell_ex,
                'ask_price':  a,
                'bid_price':  b,
                'fee':        fee,
                'pnl':        best_pnl,
            })
    trades = pd.DataFrame(records).set_index('timestamp')
    trades['cum_pnl'] = trades['pnl'].cumsum()
    return trades

# ─────────── main ───────────
def main():
    print("Building event stream…")
    events = build_event_df()
    print(f"Total events: {len(events)}")
    print("Running arbitrage backtest…")
    trades = run_arbitrage(events)
    print(f"Total trades executed: {len(trades)}")
    print(f"Total PnL: {trades['pnl'].sum():.6f} per unit")

    # plot cumulative PnL
    plt.figure(figsize=(10,6))
    plt.plot(trades.index, trades['cum_pnl'], label='Cumulative PnL')
    plt.xlabel('Time')
    plt.ylabel('P&L (per unit)')
    plt.title(f'Arbitrage Backtest P&L for {SYMBOL}')
    plt.legend()
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
