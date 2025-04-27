#!/usr/bin/env python3
"""
backtest_with_sizes.py – Decoupled arbitrage backtester with book-impact simulation

Adds bid/ask size to the trading decision and simulates the removal of top-of-book size
when a trade executes, setting price to None once filled. Additionally, only emits
new events when the bid or ask price changes for each venue.
"""
import argparse
import pandas as pd
import glob
import matplotlib.pyplot as plt
from pathlib import Path
import datetime
import time


# ─────────── configuration ───────────
SYMBOL    = 'BTC/USD'
SUBPATH = 'BTC-USD'
DATA_DIR = Path(f'data/{SUBPATH}')
EXCHANGES = ['binanceus', 'coinbase', 'hyperliquid', 'kraken', 'mexc']

# Maker fees per exchange (decimal fraction)
FEES = {
    'binanceus':   0.00, #https://www.binance.us/fees
    'coinbase':    0.0035, #https://www.coinbase.com/advanced-fees
    'hyperliquid': 0.00035, #https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees
    'kraken':      0.002, #https://www.kraken.com/features/fee-schedule
    'mexc':        0.00, #https://www.mexc.com/zero-fee
}
# FEES = {
#     'binanceus':   0.00, 
#     'coinbase':    0.00, 
#     'hyperliquid': 0.000, 
#     'kraken':      0.0, 
#     'mexc':        0.00, 
# }

# ─────────── trading logic ───────────
def trade_decision(state):
    """
    Given the latest bids/asks in `state` dict (including sizes), find the arbitrage
    trade with max positive PnL after fees and limited by available size.
    """
    best = None
    best_pnl = 0 # min EV hurdle
    volume_scale = .5 # take a fraction of the min volume to minimize slippage

    for buy_ex in EXCHANGES:
        a = state[buy_ex]['ask']
        ask_size = state[buy_ex]['ask_size']
        if a is None or ask_size is None or ask_size <= 0:
            continue

        for sell_ex in EXCHANGES:
            if sell_ex == buy_ex:
                continue

            b = state[sell_ex]['bid']
            bid_size = state[sell_ex]['bid_size']
            if b is None or bid_size is None or bid_size <= 0:
                continue

            volume = min(ask_size, bid_size) * volume_scale
            if volume <= 0:
                continue

            mid_price = (a + b) / 2
            fee_amount = (FEES[buy_ex] + FEES[sell_ex]) * mid_price * volume
            pnl = (b - a) * volume - fee_amount

            if pnl > best_pnl:
                best_pnl = pnl
                best = {
                    'buy_on':    buy_ex,
                    'sell_on':   sell_ex,
                    'ask_price': a,
                    'bid_price': b,
                    'ask_size':  ask_size,
                    'bid_size':  bid_size,
                    'volume':    volume,
                    'fee':       fee_amount,
                    'pnl':       pnl,
                }

    return best

# ─────────── build historical event stream ───────────
def load_historical_events():
    events = []
    for ex in EXCHANGES:
        pattern = f"{DATA_DIR}/{ex}_*.parquet"
        for path in glob.glob(pattern):
            df = pd.read_parquet(path)
            df = df[df['pair'] == SYMBOL]
            if df.empty:
                continue
            # convert timestamps and sort
            df = df.copy()
            df['ts'] = pd.to_datetime(df['ts_ns'], unit='ns')
            df = df.sort_values('ts')
            # only keep rows where bid and ask price changed
            df = df.loc[(df['bid'] != df['bid'].shift()) & (df['ask'] != df['ask'].shift())]
            if df.empty:
                continue
            df['venue'] = ex
            events.append(df[['ts', 'venue', 'bid', 'ask', 'bid_size', 'ask_size']])

    if not events:
        raise ValueError(f"No events for {SYMBOL}")

    all_ev = pd.concat(events, ignore_index=True)
    return all_ev.sort_values('ts').to_dict('records')

# ─────────── backtest runner ───────────
def run_backtest(event_stream):
    # initialize orderbook state
    state = {ex: {'bid': None, 'ask': None, 'bid_size': None, 'ask_size': None} for ex in EXCHANGES}
    records = []

    for ev in event_stream:
        ts = ev['ts']
        ex = ev['venue']
        # update book
        state[ex]['bid'] = ev['bid']
        state[ex]['ask'] = ev['ask']
        state[ex]['bid_size'] = ev['bid_size']
        state[ex]['ask_size'] = ev['ask_size']

        # decide trade
        trade = trade_decision(state)
        if trade:
            trade['timestamp'] = ts
            records.append(trade)

            # Print trade details
            print(f"Trade at {ts}: Buy {trade['volume']:.4f} {SYMBOL} on {trade['buy_on']} @ {trade['ask_price']}, "
                  f"Sell on {trade['sell_on']} @ {trade['bid_price']}. "
                  f"Fee: {trade['fee']:.6f}, PnL: {trade['pnl']:.6f}")

            # simulate top-of-book fill: assumes after a trade executed, the opportunity disappears regardless of how much we filled
            buy = trade['buy_on']
            sell = trade['sell_on']

            state[buy]['ask_size'] = None
            state[buy]['ask'] = None
            state[sell]['bid_size'] = None
            state[sell]['bid'] = None

    trades = pd.DataFrame(records)
    if not trades.empty:
        trades = trades.set_index('timestamp')
        trades['cum_pnl'] = trades['pnl'].cumsum()
    return trades

# ─────────── main ───────────
def main():
    print("Building historical events…")
    events = load_historical_events()
    print(f"Total events: {len(events)}")

    print("Running backtest…")
    trades = run_backtest(events)
    print(f"Trades executed: {len(trades)}")

    if not trades.empty:
        total = trades['pnl'].sum()
        print(f"Total PnL: {total:.6f}")
        plt.figure(figsize=(10,6))
        plt.plot(trades.index, trades['cum_pnl'], label='Cumulative PnL')
        plt.xlabel('Time')
        plt.ylabel('P&L')
        plt.title(f'Arbitrage PnL for {SYMBOL}')
        plt.legend()
        plt.tight_layout()
        plt.show()
    else:
        print("No profitable trades.")

if __name__ == '__main__':
    main()
