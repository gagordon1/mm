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

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import ccxt.pro as ccxt
from control import SYMBOL_MAP, EXCHANGES
import websockets
import json
import time

# ─────────── Globals (potentially redefined by args) ───────────
SYMBOL = 'ETH/USDC' # Default
SUBPATH = 'ETH-USDC' # Default
OUT = pathlib.Path(f"data/{SUBPATH}") # Default
FLUSH_INTERVAL = 5      # seconds
BUFFER_THRESHOLD = 500  # flush when this many rows accumulate


# Parquet schema definition
tableschema = pa.schema([
    ("ts_ns",    pa.int64()),
    ("pair",     pa.string()),
    ("bid",      pa.float64()),
    ("ask",      pa.float64()),
    ("bid_size", pa.float64()),
    ("ask_size", pa.float64()),
    ("venue",    pa.string()),
])

def get_current_utc_nanoseconds():
    """Returns the current UTC time as nanoseconds since the epoch."""
    return int(time.time_ns())

def parquet_path(venue: str) -> pathlib.Path:
    return OUT / f"{venue}_{datetime.date.today()}.parquet"


def append_to_parquet(df: pd.DataFrame, venue: str) -> None:
    path = parquet_path(venue)
    if path.exists():
        existing = pd.read_parquet(path)
        df = pd.concat([existing, df], ignore_index=True)
    df.to_parquet(path, schema=tableschema, compression="zstd", index=False)


class Buffer:
    def __init__(self, venue: str):
        self.venue = venue
        self.rows: list = []
        self.lock = asyncio.Lock()

    def add(self, ts_ns: int, pair: str, bid: float, ask: float, bid_size: float, ask_size: float):
        self.rows.append((ts_ns, pair, bid, ask, bid_size, ask_size, self.venue))

    async def flush(self) -> None:
        async with self.lock:
            if not self.rows:
                return
            df = pd.DataFrame(self.rows, columns=tableschema.names)
            append_to_parquet(df, self.venue)
            self.rows.clear()


async def watch_exchange(exchange_id: str, queue: asyncio.Queue):
    try:
        if exchange_id == "hyperliquid":
            await watch_hyperliquid(queue)
            return
        else:
            cls = getattr(ccxt, exchange_id)
            exchange = cls({ 'enableRateLimit': True })
            
            await exchange.load_markets()
            market = SYMBOL_MAP[SYMBOL][exchange_id]
            while True:
                try:
                    ticker = await exchange.watch_ticker(market)
                except Exception:
                    await asyncio.sleep(1)
                    continue
                bid      = ticker.get('bid')
                ask      = ticker.get('ask')
                bid_size = ticker.get('bidVolume')
                ask_size = ticker.get('askVolume')
                if bid is None or ask is None:
                    continue
                
                await queue.put((exchange_id, SYMBOL, bid, ask, bid_size, ask_size, get_current_utc_nanoseconds()))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"Error on {exchange_id}: {e}")
    finally:
        await exchange.close()

async def watch_hyperliquid(queue: asyncio.Queue):
    """
    Connects to Hyperliquid's WebSocket and streams BBO for `coin`.
    On every update it does:
        await queue.put((exchange, symbol, bid, ask, bid_size, ask_size, timestamp_ns))
    """
    uri = "wss://api.hyperliquid.xyz/ws"
    exchange_id = "hyperliquid"
    coin = SYMBOL_MAP[SYMBOL][exchange_id]
    async with websockets.connect(uri) as ws:
        # 1) subscribe to the BBO feed
        await ws.send(json.dumps({
            "method": "subscribe",
            "subscription": {
                "type": "bbo",
                "coin": coin
            }
        }))

        # 2) consume updates
        async for raw in ws:
            msg = json.loads(raw)

            # Hyperliquid labels feeds with a "channel" field
            if msg.get("channel") != "bbo":
                continue

            data = msg["data"]
            bid_level, ask_level = data.get("bbo", [None, None])

            # skip if either side is missing
            if not bid_level or not ask_level:
                continue

            # parse out floats
            bid      = float(bid_level["px"])
            ask      = float(ask_level["px"])
            bid_size = float(bid_level["sz"])
            ask_size = float(ask_level["sz"])
            ts_ns    = get_current_utc_nanoseconds()

            await queue.put((
                "hyperliquid",
                SYMBOL,
                bid,
                ask,
                bid_size,
                ask_size,
                ts_ns
            ))


async def flusher(buffers: dict):
    while True:
        await asyncio.gather(*(b.flush() for b in buffers.values()))
        await asyncio.sleep(FLUSH_INTERVAL)


async def main(quiet: bool):
    buffers = {ex: Buffer(ex) for ex in EXCHANGES}
    queue: asyncio.Queue = asyncio.Queue()

    tasks = [asyncio.create_task(watch_exchange(ex, queue)) for ex in EXCHANGES]
    tasks.append(asyncio.create_task(flusher(buffers)))

    def stop_all():
        for t in tasks:
            t.cancel()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_all())

    try:
        while True:
            venue, pair, bid, ask, bid_size, ask_size, ts_ns = await queue.get()
            buf = buffers.get(venue)
            if buf:
                buf.add(ts_ns, pair, bid, ask, bid_size, ask_size)
                if not quiet:
                    now = datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
                    print(f"{now}  {venue:<12} {bid:.8f} / {ask:.8f}   size {bid_size}/{ask_size}")
                if len(buf.rows) >= BUFFER_THRESHOLD:
                    await buf.flush()
    except asyncio.CancelledError:
        pass
    finally:
        await asyncio.gather(*(b.flush() for b in buffers.values()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Collect BBO data for a given crypto pair.")
    parser.add_argument("--coin", type=str, required=True, help="The coin symbol (e.g., BTC)")
    parser.add_argument("--base", type=str, required=True, help="The base currency symbol (e.g., USDC)")
    parser.add_argument("--minutes", type=float, default=0,
                        help="run for N minutes then exit")
    parser.add_argument("--quiet", action="store_true",
                        help="suppress live prints")
    args = parser.parse_args()

    # --- Update module-level variables based on args ---
    SYMBOL = f"{args.coin}/{args.base}"
    SUBPATH = f"{args.coin}-{args.base}"
    OUT = pathlib.Path(f"data/{SUBPATH}")
    OUT.mkdir(parents=True, exist_ok=True) # Create directory after defining OUT
    # -------------------------------------------

    print(f"Collector started for {SYMBOL}...")
    print(f"Output directory: {OUT}")

    if args.minutes > 0:
        try:
            asyncio.run(asyncio.wait_for(main(args.quiet), timeout=args.minutes * 60))
        except asyncio.TimeoutError:
            pass
        except KeyboardInterrupt:
            print("Script interrupted by user")
    else:
        try:
            asyncio.run(main(args.quiet))
        except KeyboardInterrupt:
            print("Script interrupted by user")
