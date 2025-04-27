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
    },
    'ETH/BTC' : {
        'kraken' : 'ETH/BTC',
        'hyperliquid' : None, #not supported
        'gemini' : 'ETH/BTC',
        'coinbase' : 'ETH/BTC',
        'binanceus' : 'ETH/BTC',
        'bitfinex' : 'ETH/BTC',
        'mexc' : 'ETH/BTC'
    }
}

# Fee structure: { exchange: { pair: fee_rate } }
# Note: These are example rates based on previous flat fees.
# VERIFY AND UPDATE these with actual pair-specific taker fees.
FEES = {
    'binanceus': {
        'BTC/USDC': 0.00,
        'ETH/USDC': 0.00,
        'ETH/BTC': 0.00,
        # Add other pairs as needed
    },
    'coinbase': {
        'BTC/USDC': 0.0035,
        'ETH/USDC': 0.0035,
        'ETH/BTC': 0.0035,
    },
    'hyperliquid': {
        'BTC/USDC': 0.00035, # Assuming '@142' corresponds to BTC/USDC
        'ETH/USDC': 0.00035, # Assuming '@151' corresponds to ETH/USDC
        # 'ETH/BTC': None, # Not supported
    },
    'kraken': {
        'BTC/USDC': 0.002,
        'ETH/USDC': 0.002,
        'ETH/BTC': 0.002,
    },
    'mexc': {
        'BTC/USDC': 0.00,
        'ETH/USDC': 0.00,
        'ETH/BTC': 0.0005, #https://www.mexc.com/exchange/ETH_BTC
    },
    'gemini': {
        'BTC/USDC': 0.004,
        'ETH/USDC': 0.004,
        'ETH/BTC': 0.004,
    }
}

# FEES = {
#     'binanceus':   0.00, #https://www.binance.us/fees
#     'coinbase':    0.0, #https://www.coinbase.com/advanced-fees
#     'hyperliquid': 0.00, #https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees
#     'kraken':      0.00, #https://www.kraken.com/features/fee-schedule
#     'mexc':        0.00, #https://www.mexc.com/zero-fee
#     # Add 'gemini' if it's used and fee is known, otherwise remove from EXCHANGES in control.py
#     'gemini': 0.00 # Example fee - VERIFY THIS: https://www.gemini.com/fees/activetrader-fee-schedule#section-overview
# }