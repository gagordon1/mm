
#!/usr/bin/env python3
"""
collector.py – ETH/USDC best-bid/ask printer + Parquet logger using ccxt.pro

Streams and prints every top-of-book update for ETH/USDC from:
  • Kraken
  • Coinbase Advanced Trade
  • Binance US
  • Gemini
  • Bitfinex
  • Hyperliquid (append :USDC suffix)

Uses ccxt.pro for unified websocket interfaces. Stores rows to per-venue daily Parquet files.
Each row schema: ts_ns | pair | bid | ask | venue
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
from symbol_map import SYMBOL_MAP
# ─────────── configuration ───────────
OUT = pathlib.Path("data"); OUT.mkdir(exist_ok=True)
FLUSH_INTERVAL = 5      # seconds
BUFFER_THRESHOLD = 500  # flush when this many rows accumulate
SYMBOL = 'BTC/USD'
EXCHANGES = ['kraken', 'coinbase', 'hyperliquid', 'binanceus', 'gemini']

# Parquet schema definition
tableschema = pa.schema([
    ("ts_ns", pa.int64()),
    ("pair" , pa.string()),
    ("bid"  , pa.float64()),
    ("ask"  , pa.float64()),
    ("venue", pa.string()),
])


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

    def add(self, ts_ns: int, pair: str, bid: float, ask: float):
        self.rows.append((ts_ns, pair, bid, ask, self.venue))

    async def flush(self) -> None:
        async with self.lock:
            if not self.rows:
                return
            df = pd.DataFrame(self.rows, columns=tableschema.names)
            append_to_parquet(df, self.venue)
            self.rows.clear()


async def watch_exchange(exchange_id: str, queue: asyncio.Queue):
    cls = getattr(ccxt, exchange_id)
    exchange = cls({ 'enableRateLimit': True })
    try:
        await exchange.load_markets()
        market = SYMBOL_MAP[SYMBOL][exchange_id]
        # stream
        while True:
            try:
                ticker = await exchange.watch_ticker(market)
            except Exception:
                await asyncio.sleep(1)
                continue
            bid = ticker.get('bid')
            ask = ticker.get('ask')
            if bid is None or ask is None:
                continue
            ts_ns = int(exchange.milliseconds() * 1_000_000)
            await queue.put((exchange_id, SYMBOL, bid, ask, ts_ns))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"Error on {exchange_id}: {e}")
    finally:
        await exchange.close()


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
            venue, pair, bid, ask, ts_ns = await queue.get()
            buf = buffers.get(venue)
            if buf:
                buf.add(ts_ns, pair, bid, ask)
                if not quiet:
                    now = datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
                    print(f"{now}  {venue:<12} {bid:.8f} / {ask:.8f}")
                if len(buf.rows) >= BUFFER_THRESHOLD:
                    await buf.flush()
    except asyncio.CancelledError:
        pass
    finally:
        await asyncio.gather(*(b.flush() for b in buffers.values()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=0,
                        help="run for N minutes then exit")
    parser.add_argument("--quiet", action="store_true",
                        help="suppress live prints")
    args = parser.parse_args()

    if args.minutes > 0:
        try:
            asyncio.run(asyncio.wait_for(main(args.quiet), timeout=args.minutes * 60))
        except asyncio.TimeoutError:
            pass
    else:
        asyncio.run(main(args.quiet))
