#!/usr/bin/env python3
"""
backtest_with_sizes.py – Decoupled arbitrage backtester with book-impact simulation

Supports multiple trading pairs in the MarketState and pluggable trade strategies.
"""
import argparse
import pandas as pd
import glob
import matplotlib.pyplot as plt
from pathlib import Path
from control import EXCHANGES
from strategies import cross_exchange_arbitrage, triangle_arbitrage, MarketState, Trade
from typing import Callable, Optional, List, Dict


def load_historical_events(data_dir: Path) -> List[dict]:
    events: List[dict] = []
    # Load all parquet files in directory
    for path in glob.glob(f"{data_dir}/*.parquet"):
       
        df = pd.read_parquet(path)
        # Keep required columns (using 'pair' as the trading pair key)
        df = df[['ts_ns', 'venue', 'pair', 'bid', 'ask', 'bid_size', 'ask_size']].copy() 
        # Convert timestamp and sort
        df['ts'] = pd.to_datetime(df['ts_ns'], unit='ns')
        df = df.sort_values('ts')
        # Filter to only price/size changes per venue+pair
        df['prev_bid'] = df.groupby(['venue', 'pair'])['bid'].shift()
        df['prev_ask'] = df.groupby(['venue', 'pair'])['ask'].shift()
        df = df.loc[(df['bid'] != df['prev_bid']) | (df['ask'] != df['prev_ask'])]
        if df.empty:
            continue
        # Append each row as an event dict
        for _, row in df.iterrows():
            events.append({
                'ts': row['ts'],
                'venue': row['venue'],
                'pair': row['pair'],
                'bid': row['bid'],
                'ask': row['ask'],
                'bid_size': row['bid_size'],
                'ask_size': row['ask_size'],
            })
    if not events:
        raise ValueError(f"No events found in {data_dir}")
    # Sort events chronologically
    return sorted(events, key=lambda x: x['ts'])


def parse_pair(pair_str: str) -> tuple[str, str]:
    """Parses a pair string like 'BTC/USD' into base and quote."""
    try:
        base, quote = pair_str.split('/')
        return base.strip(), quote.strip()
    except ValueError:
        raise ValueError(f"Invalid pair format: {pair_str}. Expected format like 'BASE/QUOTE'.")


def run_backtest(
    event_stream: List[dict],
    trade_strategy: Callable[[MarketState], List[Trade]]
) -> pd.DataFrame:
    # Initialize empty MarketState and Positions (by Coin)
    state: MarketState = {ex: {} for ex in EXCHANGES}
    # {venue: {coin: balance}}
    positions: Dict[str, Dict[str, float]] = {ex: {} for ex in EXCHANGES}
    records: List[Trade] = []

    # Assume starting with 0 balance for all coins involved
    # (Balances will be created lazily when first encountered)

    for ev in event_stream:
        ts = ev['ts']
        ex = ev['venue']
        pair = ev['pair']
        # Initialize ticker dict if missing
        if pair not in state[ex]:
            state[ex][pair] = {'bid': None, 'ask': None, 'bid_size': None, 'ask_size': None}
        # Update book state
        ticker = state[ex][pair]
        ticker['bid'] = ev['bid']
        ticker['ask'] = ev['ask']
        ticker['bid_size'] = ev['bid_size']
        ticker['ask_size'] = ev['ask_size']

        # Invoke strategy on full MarketState
        potential_trades: List[Trade] = trade_strategy(state)

        # Process each trade leg returned by the strategy
        if potential_trades:
            print("---"*5)
            for trade in potential_trades:
                # Stamp executed time as float seconds
                trade['timestamp'] = ts.timestamp()
                records.append(trade)

                # --- Update Positions by Coin ---
                venue = trade['venue']
                pair = trade['pair']
                volume = trade['volume']
                price = trade['price']
                side = trade['side']
                
                try:
                    base_coin, quote_coin = parse_pair(pair)
                except ValueError as e:
                    print(f"Skipping position update due to error: {e}")
                    continue # Skip this trade for position tracking

                # Ensure venue exists in positions (should always be true)
                if venue not in positions:
                     positions[venue] = {}
                     
                # Get current balances (default to 0 if coin not seen before)
                base_balance = positions[venue].get(base_coin, 0.0)
                quote_balance = positions[venue].get(quote_coin, 0.0)
                
                cost_or_proceeds = volume * price

                if side == 'buy': # Buying base coin, selling quote coin
                    positions[venue][base_coin] = base_balance + volume
                    positions[venue][quote_coin] = quote_balance - cost_or_proceeds
                elif side == 'sell': # Selling base coin, buying quote coin
                    positions[venue][base_coin] = base_balance - volume
                    positions[venue][quote_coin] = quote_balance + cost_or_proceeds
                # --- End Position Update ---

                # Print trade details
                print(
                    f"Trade at {ts}: {side.upper()} {volume:.4f} {pair} on {venue} @ {price:.4f}. "
                    f"Fee: {trade['fee']:.6f}"
                )

                # Simulate top-of-book removal based on trade leg
                traded_pair = trade['pair']
                trade_venue = trade['venue']
                if traded_pair in state[trade_venue]:
                    if trade['side'] == 'buy':
                        state[trade_venue][traded_pair]['ask'] = None
                        state[trade_venue][traded_pair]['ask_size'] = None
                    elif trade['side'] == 'sell':
                        state[trade_venue][traded_pair]['bid'] = None
                        state[trade_venue][traded_pair]['bid_size'] = None

    # Print final positions (balances per coin)
    print("\nFinal Positions (Coin Balances):")
    has_positions = False
    for venue, coins in positions.items():
        venue_has_positions = False
        for coin, balance in coins.items():
             if balance != 0:
                 if not venue_has_positions:
                     print(f"  {venue}:")
                     venue_has_positions = True
                     has_positions = True
                 print(f"    {coin}: {balance:.8f}") # Increased precision for balances
    if not has_positions:
        print("  (No non-zero balances)")

    # Compile trade records into DataFrame (no PnL calculation here)
    trades_df = pd.DataFrame(records)
    if not trades_df.empty:
        trades_df = trades_df.set_index('timestamp')

    return trades_df


def main():
    parser = argparse.ArgumentParser(
        description="Run multi-pair arbitrage backtest on collected data."
    )
    parser.add_argument(
        "--path", type=str, required=True,
        help="Path to directory containing parquet files with order book snapshots"
    )
    parser.add_argument(
        "--strategy", choices=['cross', 'tri'], default='cross',
        help="Choose trading strategy: 'cross' for cross-exchange, 'tri' for triangular"
    )
    args = parser.parse_args()

    data_dir = Path(f"data/{args.path}")
    print(f"Loading events from {data_dir}...")
    try:
        events = load_historical_events(data_dir)
    except ValueError as e:
        print(f"Error loading events: {e}")
        return
    except FileNotFoundError:
        print(f"Error: Data directory not found at {data_dir}")
        return

    print(f"Total events loaded: {len(events)}")

    # Select strategy function
    strategy_fn = (
        cross_exchange_arbitrage if args.strategy == 'cross'
        else triangle_arbitrage
    )

    print("\nRunning backtest…")
    trades = run_backtest(events, strategy_fn)
    print(f"\nTotal trade legs executed: {len(trades)}")

    # PnL reporting and plotting removed
    if not trades.empty:
        # Optional: Save trades log to csv
        # trades.to_csv('backtest_trades.csv')
        # print("Trade log saved to backtest_trades.csv")
        pass # No PnL to report here
    else:
        print("No trades executed.")

if __name__ == '__main__':
    main()
