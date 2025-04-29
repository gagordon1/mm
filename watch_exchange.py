import asyncio
import json
import websockets
from typing import Optional
import pathlib
from utils import get_current_utc_nanoseconds
from control import SYMBOL_MAP
import ccxt.pro as ccxt

async def watch_exchange(exchange_id: str, symbol: str, output_dir: pathlib.Path, queue: asyncio.Queue):
    cls = getattr(ccxt, exchange_id)
    exchange = cls({'enableRateLimit': True})
    if exchange_id not in SYMBOL_MAP.get(symbol, {}):
        print(f"{symbol} not found in SYMBOL_MAP for {exchange_id}")
        await exchange.close()
        return
    market = SYMBOL_MAP[symbol][exchange_id]
    if market is None:
        print(f"{symbol} explicitly not supported on {exchange_id} in SYMBOL_MAP")
        await exchange.close()
        return
        
    while True:
        try:
            ticker = await exchange.watch_ticker(market)
            bid = ticker.get('bid')
            ask = ticker.get('ask')
            bid_size = ticker.get('bidVolume') 
            ask_size = ticker.get('askVolume')
            if bid is None or ask is None:
                 continue                 
            ts_ns = get_current_utc_nanoseconds()
            await queue.put((exchange_id, symbol, bid, ask, bid_size, ask_size, None, ts_ns))
        except asyncio.CancelledError:
            print(f"[{exchange_id}] Watch task cancelled.")
            break 
        except Exception as e:
            print(f"Error in watch_exchange {exchange_id}: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
    await exchange.close()
    print(f"[{exchange_id}] Connection closed.")

async def watch_hyperliquid(symbol: str, output_dir: pathlib.Path, queue: asyncio.Queue):
    uri = "wss://api.hyperliquid.xyz/ws"
    exchange_id = "hyperliquid"
    if exchange_id not in SYMBOL_MAP.get(symbol, {}):
        print(f"{symbol} not found in SYMBOL_MAP for {exchange_id}")
        return
    coin = SYMBOL_MAP[symbol][exchange_id]
    if coin is None:
        print(f"{symbol} explicitly not supported on {exchange_id} in SYMBOL_MAP")
        return
        
    while True: 
        try:
            async with websockets.connect(uri) as ws:
                print(f"[{exchange_id}] Connected.")
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "bbo", "coin": coin}
                }))
                print(f"[{exchange_id}] Subscribed to BBO for {coin}.")

                async for message_raw in ws:
                    try:
                        msg = json.loads(message_raw)
                        if msg.get("channel") != "bbo": continue
                        data = msg.get("data", {})
                        bbo_data = data.get("bbo", [None, None])
                        bid_level, ask_level = bbo_data
                        if not bid_level or not ask_level: continue
                        bid = float(bid_level["px"])
                        ask = float(ask_level["px"])
                        bid_size = float(bid_level["sz"])
                        ask_size = float(ask_level["sz"])
                        ts_ns = get_current_utc_nanoseconds()
                        await queue.put((exchange_id, symbol, bid, ask, bid_size, ask_size, None, ts_ns))
                    except json.JSONDecodeError:
                        print(f"[{exchange_id}] Non-JSON msg: {message_raw[:100]}...")
                    except Exception as e:
                        print(f"[{exchange_id}] Processing error: {e} - Msg: {message_raw[:100]}...")
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosed, OSError) as e:
            print(f"[{exchange_id}] Connection error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            print(f"[{exchange_id}] Watch task cancelled.")
            break
        except Exception as e:
             print(f"[{exchange_id}] Unexpected error: {e}. Retrying in 15s...")
             await asyncio.sleep(15)
    print(f"[{exchange_id}] Connection closed.")

async def watch_gemini(symbol: str, output_dir: pathlib.Path, queue: asyncio.Queue):
    """
    Native Gemini Market Data WS (no ccxt):
      wss://api.gemini.com/v1/marketdata/{symbol}?top_of_book=true&heartbeat=true
    Emits (venue, pair, bid, ask, bid_size, ask_size, funding_rate, ts_ns).
    """
    exchange_id = "gemini"
    raw = SYMBOL_MAP.get(symbol, {}).get(exchange_id)
    print(f"[{exchange_id}] raw: {raw}")
    if not raw:
        print(f"{symbol} not supported on {exchange_id}")
        return

    # Gemini symbols are CCY1CCY2 lowercase, no slash
    market = raw.replace('/', '').lower()
    uri = (
        f"wss://api.gemini.com/v1/marketdata/"
        f"{market}?top_of_book=true&heartbeat=true"
    )
    print(f"[{exchange_id}] Connecting to {uri}")

    while True:
        try:
            async with websockets.connect(uri) as ws:
                print(f"[{exchange_id}] Connected.")
                bid = ask = bid_size = ask_size = None

                async for raw_msg in ws:
                    msg = json.loads(raw_msg)
                    mtype = msg.get("type")
                    # We care about first “initial” snapshot and subsequent “update”
                    if mtype not in ("initial", "update"):
                        continue

                    for ev in msg.get("events", []):
                        if ev.get("type") != "change":
                            continue
                        side = ev["side"]
                        price = float(ev["price"])
                        size  = float(ev["remaining"])
                        if side == "bid":
                            bid, bid_size = price, size
                        elif side == "ask":
                            ask, ask_size = price, size

                    # Once both bid and ask are known, emit the row
                    if bid is not None and ask is not None:
                        ts_ns = get_current_utc_nanoseconds()
                        await queue.put((
                            exchange_id,
                            symbol,
                            bid, ask,
                            bid_size, ask_size,
                            None,        # no perpetual funding here
                            ts_ns
                        ))

        except asyncio.CancelledError:
            print(f"[{exchange_id}] Watch task cancelled.")
            break

        except Exception as e:
            print(f"[{exchange_id}] error: {e}. reconnecting in 5s…")
            await asyncio.sleep(5)

    print(f"[{exchange_id}] Connection closed.")



