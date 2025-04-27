import ccxt.pro as ccxt
import asyncio

EXCHANGES = ['binanceus', 'coinbase', 'hyperliquid', 'kraken', 'mexc','gemini']

SYMBOL_MAP = {
    'BTC/USDC' : {
        'kraken' : 'BTC/USDC',
        'hyperliquid' : '@142', #UBTC/USDC
        'gemini' : 'BTC/USDC',
        'coinbase' : 'BTC/USDC',
        'binanceus' : 'BTC/USDC',
        'bitfinex' : 'BTC/USDC',
        'mexc' : 'BTC/USDC'
    },
    'ETH/USDC' : {
        'kraken' : 'ETH/USDC',
        'hyperliquid' : '@151', #UETH/USDC
        'gemini' : 'ETH/USD',
        'coinbase' : 'ETH/USDC',
        'binanceus' : 'ETH/USDC',
        'bitfinex' : 'ETH/USDC',
        'mexc' : 'ETH/USDC'
    },
    'SOL/USDC' : {
        'kraken' : 'SOL/USDC',
        'hyperliquid' : '@156', #USOL/USDC
        'gemini' : 'SOL/USD',
        'coinbase' : 'SOL/USDC',
        'binanceus' : 'SOL/USDC',
        'bitfinex' : 'SOL/USDC',
        'mexc' : 'SOL/USDC'
    }
}