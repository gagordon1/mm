#!/usr/bin/env python3
"""
perp_collector.py – Perpetual futures BBO + Funding Rate logger.

Streams and logs Best Bid/Offer and Funding Rate updates for a specified 
perpetual contract (e.g., BTC-PERP) from Hyperliquid.

Uses direct websocket connection. Stores rows to Parquet files.
Schema: ts_ns | pair | bid | ask | bid_size | ask_size | funding_rate | venue
"""
import asyncio
import signal
import argparse
import datetime
import pathlib
import json
import time
from typing import Dict, List, Optional, Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import websockets

# --- Import reusable components from spot collector --- 
# Assuming spot_collector.py contains the unified versions
from spot_collector import (
    Buffer,
)
# --- --- --- 
from utils import get_current_utc_nanoseconds
# --- Globals ---
FLUSH_INTERVAL = 10      # seconds
BUFFER_THRESHOLD = 500  # flush when this many rows accumulate
VENUE = "hyperliquid-perp" # Focused on Hyperliquid for now

# --- State for latest funding rate ---
# Needs to be accessed by the BBO handler
latest_funding_rates: Dict[str, float] = {} # {ticker: rate}

# --- Parquet schema definition --- (REMOVED - Imported)
# tableschema = ... 

# --- File Writing --- (REMOVED - Imported)
# def append_to_parquet(...): ...

# --- Data Buffering --- (REMOVED - Imported)
# class Buffer(...): ...

# --- WebSocket Handling (Hyperliquid) ---
async def watch_hyperliquid_perp(ticker: str, queue: asyncio.Queue):
    """
    Connects to Hyperliquid, subscribes to BBO and Funding for `ticker`,
    and puts BBO data onto the queue, including the latest funding rate.
    Updates the global latest_funding_rates dict.
    """
    uri = "wss://api.hyperliquid.xyz/ws"
    coin = ticker # Assuming ticker format is suitable (e.g., "BTC")

    while True: # Reconnection loop
        try:
            async with websockets.connect(uri) as ws:
                print(f"[{VENUE}] Connected.")
                # Ensure ticker key exists, initialize to 0.0 if not present
                if ticker not in latest_funding_rates:
                    latest_funding_rates[ticker] = 0.0 

                # Subscribe to BBO
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "bbo", "coin": coin}
                }))
                print(f"[{VENUE}] Subscribed to BBO for {coin}.")

                # Subscribe to Funding
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "activeAssetCtx", "coin": coin}
                }))
                print(f"[{VENUE}] Subscribed to Funding for {coin}.")

                async for message_raw in ws:
                    try:
                        msg = json.loads(message_raw)
                        channel = msg.get("channel")

                        if channel == "bbo":
                            data = msg.get("data", {})
                            bbo_data = data.get("bbo", [None, None])
                            bid_level, ask_level = bbo_data

                            if not bid_level or not ask_level: continue

                            bid = float(bid_level["px"])
                            ask = float(ask_level["px"])
                            bid_size = float(bid_level["sz"])
                            ask_size = float(ask_level["sz"])
                            ts_ns = get_current_utc_nanoseconds()
                            current_funding = latest_funding_rates.get(ticker)

                            # Put BBO data with the latest known funding rate
                            await queue.put((VENUE, ticker, bid, ask, bid_size, ask_size, current_funding, ts_ns))

                        elif channel == "activeAssetCtx":
                            try:
                                funding = float(msg["data"]["ctx"]["funding"])
                                latest_funding_rates[ticker] = funding
                                # Optional: print funding update
                                # print(f"[{VENUE}] Funding Rate Update for {ticker}: {funding:+.6%}")
                            except (KeyError, TypeError, ValueError) as e:
                                print(f"[{VENUE}] Error parsing funding rate: {e} - Data: {msg}")

                    except json.JSONDecodeError:
                        print(f"[{VENUE}] Received non-JSON message: {message_raw[:100]}...")
                    except Exception as e:
                        print(f"[{VENUE}] Error processing message: {e} - Message: {message_raw[:100]}...")

        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosed, OSError) as e:
            print(f"[{VENUE}] WebSocket connection error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            print(f"[{VENUE}] Watch task cancelled.")
            break
        except Exception as e:
            print(f"[{VENUE}] Unexpected error in watch_hyperliquid: {e}. Reconnecting in 15 seconds...")
            await asyncio.sleep(15) # Longer sleep for unexpected


# --- Periodic Flusher ---
async def flusher(buffer: Buffer):
    """Periodically flushes the buffer."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        await buffer.flush()


# --- Main Execution ---
async def main(ticker: str, output_path_str: str, quiet: bool):
    
    output_dir = pathlib.Path(output_path_str)
    output_dir.mkdir(parents=True, exist_ok=True) # Create directory
    print(f"Output directory: {output_dir.resolve()}")

    buffer = Buffer(VENUE, output_dir)
    queue: asyncio.Queue = asyncio.Queue()

    # Start the websocket listener task
    watch_task = asyncio.create_task(watch_hyperliquid_perp(ticker, queue))
    # Start the periodic flusher task
    flush_task = asyncio.create_task(flusher(buffer))

    tasks = [watch_task, flush_task]

    def stop_all():
        print("Stopping tasks...")
        for t in tasks:
            t.cancel()
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_all)

    print(f"Collector started for {ticker} on {VENUE}...")

    try:
        while True:
            # Get data from the queue (populated by watch_hyperliquid)
            # Tuple: (venue, ticker, bid, ask, bid_size, ask_size, funding_rate, ts_ns)
            venue, pair, bid, ask, bid_size, ask_size, funding_rate, ts_ns = await queue.get()
            
            # Add to buffer
            buffer.add(ts_ns, pair, bid, ask, bid_size, ask_size, funding_rate)
            
            # Optional: Print live updates
            if not quiet:
                 now = datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
                 funding_str = f"{funding_rate:+.6%}" if funding_rate is not None else "N/A"
                 print(f"{now} {venue:<12} {pair:<15} {bid:.4f}/{ask:.4f}  Funding: {funding_str}")
            
            # Check buffer size
            if len(buffer.rows) >= BUFFER_THRESHOLD:
                await buffer.flush()
                
            queue.task_done() # Signal that the item is processed

    except asyncio.CancelledError:
        print("Main loop cancelled.")
    finally:
        print("Flushing remaining buffer...")
        await buffer.flush() # Final flush
        print("Collector finished.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Collect Perpetual BBO and Funding Rate data.")
    parser.add_argument("--coin", type=str, required=True, 
                        help="The perpetual ticker symbol (e.g., BTC-PERP)")
    parser.add_argument("--path", type=str, required=True, 
                        help="The directory path to save Parquet files")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress live print updates to console")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.coin, f"data/{args.path}", args.quiet))
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
