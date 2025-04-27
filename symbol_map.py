import ccxt.pro as ccxt
import asyncio

async def main():
    exchange_id = 'mexc'
    cls = getattr(ccxt, exchange_id)
    exchange = cls({ 'enableRateLimit': True })
    try:
        await exchange.load_markets()
        print(exchange.symbols)
    except Exception as e:
        print(e)
    finally:
        await exchange.close()


SYMBOL_MAP = {
    'BTC/USD' : {
        'kraken' : 'BTC/USD',
        'hyperliquid' : 'BTC',
        'gemini' : 'BTC/GUSD',
        'coinbase' : 'BTC/USD',
        'binanceus' : 'BTC/USD',
        'bitfinex' : 'BTC/USD',
        'mexc' : 'BTC/USDC'
    },
    'ETH/USD' : {
        'kraken' : 'ETH/USD',
        'hyperliquid' : 'ETH',
        'gemini' : 'ETH/GUSD',
        'coinbase' : 'ETH/USD',
        'binanceus' : 'ETH/USD',
        'bitfinex' : 'ETH/USD',
        'mexc' : 'ETH/USDC'
    }
}

if __name__ == "__main__":
    asyncio.run(main())