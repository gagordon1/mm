import ccxt.pro as ccxt
import asyncio

async def main():
    exchange_id = 'gemini'
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
        'hyperliquid' : 'BTC/USDC:USDC',
        'gemini' : 'BTC/GUSD',
        'coinbase' : 'BTC/USD',
        'binanceus' : 'BTC/USD',
        'bitfinex' : 'BTC/USD'
    },
    'ETH/USD' : {
        'kraken' : 'ETH/USD',
        'hyperliquid' : 'ETH/USDC:USDC',
        'gemini' : 'ETH/GUSD',
        'coinbase' : 'ETH/USD',
        'binanceus' : 'ETH/USD',
        'bitfinex' : 'ETH/USD'
    }
}

if __name__ == "__main__":
    asyncio.run(main())