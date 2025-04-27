import ccxt.pro as ccxt
import asyncio
# List all supported exchanges in ccxt.pro
print("Available ccxt.pro exchanges:", ccxt.exchanges)

# ─ rest of your imports and code ─
async def _fetch_tickers_async(exchange_id: str) -> dict:
    """
    Internal coroutine to fetch all tickers from a ccxt.pro exchange.
    """
    exchange_cls = getattr(ccxt, exchange_id)
    exchange = exchange_cls({'enableRateLimit': True})
    
    try:
        return await exchange.fetch_tickers()
    finally:
        await exchange.close()

x = asyncio.run(_fetch_tickers_async("coinbase"))
print([i for i in x.keys()])