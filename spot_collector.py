#!/usr/bin/env python3
"""
collector.py – ETH/USDC best-bid/ask printer + Parquet logger using ccxt.pro

Streams and prints every top-of-book update for BTC/USD from:
  • Kraken
  • Coinbase Advanced Trade
  • Binance US
  • Gemini
  • Bitfinex
  • Hyperliquid

Uses ccxt.pro for unified websocket interfaces. Stores rows to per-venue daily Parquet files.
Each row schema: ts_ns | pair | bid | ask | bid_size | ask_size | venue
"""
import asyncio
import signal
import argparse
import datetime
import pathlib
from typing import Dict, List, Optional, Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import ccxt.pro as ccxt
from control import SYMBOL_MAP, EXCHANGES
import time
from watch_exchange import watch_hyperliquid, watch_gemini, watch_exchange
from utils import get_daily_filename

# ─────────── Globals (potentially redefined by args) ───────────
SYMBOL = 'ETH/USDC' # Default
SUBPATH = 'ETH-USDC' # Default
OUT = pathlib.Path(f"data/{SUBPATH}") # Default
FLUSH_INTERVAL = 5      # seconds
BUFFER_THRESHOLD = 500  # flush when this many rows accumulate


# Parquet schema definition (Unified)
tableschema = pa.schema([
    ("ts_ns",        pa.int64()),
    ("pair",         pa.string()),
    ("bid",          pa.float64()),
    ("ask",          pa.float64()),
    ("bid_size",     pa.float64()),
    ("ask_size",     pa.float64()),
    ("funding_rate", pa.float64()),
    ("venue",        pa.string()),
])


def append_to_parquet(df: pd.DataFrame, output_dir: pathlib.Path, venue: str) -> None:
    """Appends a DataFrame to the daily Parquet file using the unified schema."""
    if df.empty:
        return
    # Use the output_dir passed as argument
    output_file = get_daily_filename(output_dir, venue) 
    try:
        # Ensure funding_rate column exists and is float, handling potential NaNs
        if 'funding_rate' not in df.columns:
             df['funding_rate'] = float('nan')
        # Ensure float type for funding rate
        df['funding_rate'] = df['funding_rate'].astype(float) 
        # Ensure float type for sizes, handling potential NaNs introduced earlier
        df['bid_size'] = df['bid_size'].astype(float)
        df['ask_size'] = df['ask_size'].astype(float)

        table = pa.Table.from_pandas(df, schema=tableschema, preserve_index=False)
        
        if output_file.exists():
            try:
                # Read existing data, concatenate, and overwrite
                existing_table = pq.read_table(output_file, schema=tableschema)
                combined_table = pa.concat_tables([existing_table, table])
                pq.write_table(combined_table, output_file, write_statistics=False, compression="zstd")
            except Exception as read_err:
                print(f"Error reading/appending {output_file}: {read_err}. Overwriting.")
                pq.write_table(table, output_file, write_statistics=False, compression="zstd")
        else:
            pq.write_table(table, output_file, write_statistics=False, compression="zstd")
            print(f"Created new file: {output_file}")

    except Exception as e:
        print(f"Error writing to Parquet file {output_file}: {e}")


class Buffer:
    def __init__(self, venue: str, output_dir: pathlib.Path):
        self.venue = venue
        self.output_dir = output_dir
        self.rows: list = []
        self.lock = asyncio.Lock()

    def add(self, ts_ns: int, pair: str, bid: float, ask: float, bid_size: Optional[float], ask_size: Optional[float], funding_rate: Optional[float]):
        funding_val = funding_rate if funding_rate is not None else float('nan')
        bid_size_val = bid_size if bid_size is not None else float('nan')
        ask_size_val = ask_size if ask_size is not None else float('nan')
        self.rows.append((ts_ns, pair, bid, ask, bid_size_val, ask_size_val, funding_val, self.venue))

    async def flush(self) -> None:
        async with self.lock:
            if not self.rows:
                return
            df = pd.DataFrame(self.rows, columns=tableschema.names)
            append_to_parquet(df, self.output_dir, self.venue)
            self.rows.clear()


async def flusher(buffers: Dict[str, Buffer]):
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        for buf in buffers.values():
            await buf.flush()

async def main(symbol: str, output_dir: pathlib.Path, quiet: bool):
    buffers = {ex: Buffer(ex, output_dir) for ex in EXCHANGES}
    queue: asyncio.Queue = asyncio.Queue()
    tasks = []

    for ex_id in EXCHANGES:
        if ex_id == "hyperliquid":
            task = asyncio.create_task(watch_hyperliquid(symbol, output_dir, queue))
        elif ex_id == "gemini":
            task = asyncio.create_task(watch_gemini(symbol, output_dir, queue))
        else:
            task = asyncio.create_task(watch_exchange(ex_id, symbol, output_dir, queue))
        tasks.append(task)
        
    tasks.append(asyncio.create_task(flusher(buffers)))

    def stop_all():
        print("Stopping tasks...")
        for t in tasks:
            t.cancel()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_all)

    print(f"Spot Collector started for {symbol}...")

    try:
        while True:
            venue, pair, bid, ask, bid_size, ask_size, funding_rate, ts_ns = await queue.get()
            buf = buffers.get(venue)
            if buf:
                buf.add(ts_ns, pair, bid, ask, bid_size, ask_size, funding_rate)
                if not quiet:
                    now = datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
                    bid_size_str = f"{bid_size:.4f}" if bid_size is not None else "N/A"
                    ask_size_str = f"{ask_size:.4f}" if ask_size is not None else "N/A"
                    print(f"{now}  {venue:<12} {bid:.8f} / {ask:.8f}   size {bid_size_str}/{ask_size_str}")
                if len(buf.rows) >= BUFFER_THRESHOLD:
                    await buf.flush()
            queue.task_done()
    except asyncio.CancelledError:
        print("Main loop cancelled.")
    finally:
        print("Flushing remaining buffers...")
        await asyncio.gather(*(b.flush() for b in buffers.values()), return_exceptions=True)
        print("Spot Collector finished.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Collect BBO data for a given crypto pair.")
    parser.add_argument("--coin", type=str, required=True, help="The coin symbol (e.g., BTC)")
    parser.add_argument("--base", type=str, required=True, help="The base currency symbol (e.g., USDC)")
    parser.add_argument("--path", type=str, required=True, help="The subpath for the output directory")
    parser.add_argument("--quiet", action="store_true",
                        help="suppress live prints")
    args = parser.parse_args()

    # --- Update module-level variables based on args ---
    SYMBOL = f"{args.coin}/{args.base}"
    SAVE_SYMBOL = f"{args.coin}{args.base}"
    SUBPATH = args.path
    OUT = pathlib.Path(f"data/{SUBPATH}")
    OUT.mkdir(parents=True, exist_ok=True) # Create directory after defining OUT
    # -------------------------------------------

    print(f"Collector started for {SYMBOL}...")
    print(f"Output directory: {OUT}")


    try:
        asyncio.run(main(SYMBOL, OUT, args.quiet))
    except KeyboardInterrupt:
        print("Script interrupted by user")
