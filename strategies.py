from typing import Dict, Optional, TypedDict, List
from control import FEES

# Type definition for the state of a single exchange's order book level for a given trading pair
Ticker = TypedDict('Ticker', {
    'bid':       Optional[float],
    'ask':       Optional[float],
    'bid_size':  Optional[float],
    'ask_size':  Optional[float]
})

# Type definition for the overall market state
# Structure: { exchange_name: { pair: Ticker } }
MarketState = Dict[str, Dict[str, Ticker]]

# Type definition for a single trade leg in an arbitrage strategy
Trade = TypedDict('Trade', {
    'pair':      str,
    'venue':     str,
    'side':      str,  # 'buy' or 'sell'
    'price':     float,
    'volume':    float,
    'fee':       float,
    'timestamp': Optional[float]
})


def cross_exchange_arbitrage(state: MarketState) -> List[Trade]:
    """
    Finds the best cross-exchange arbitrage opportunity.
    Returns two Trade objects (buy leg, sell leg) for the best trade.
    """
    best = None
    best_pnl = 0.0
    volume_scale = 1.0

    for buy_ex, tickers in state.items():
        for pair, buy_t in tickers.items():
            ask_price = buy_t.get('ask'); ask_size = buy_t.get('ask_size')
            if ask_price is None or ask_size is None or ask_size <= 0:
                continue
            for sell_ex, sell_tickers in state.items():
                if sell_ex == buy_ex:
                    continue
                sell_t = sell_tickers.get(pair)
                if not sell_t:
                    continue
                bid_price = sell_t.get('bid'); bid_size = sell_t.get('bid_size')
                if bid_price is None or bid_size is None or bid_size <= 0:
                    continue

                volume = min(ask_size, bid_size) * volume_scale
                
                # --- Get pair-specific fees --- 
                buy_fee_rate = FEES.get(buy_ex, {}).get(pair)
                sell_fee_rate = FEES.get(sell_ex, {}).get(pair)
                # --- Check if fees exist for this pair on both exchanges --- 
                if buy_fee_rate is None or sell_fee_rate is None:
                    # print(f"Warning: Missing fee rate for {pair} on {buy_ex} or {sell_ex}") # Optional warning
                    continue 
                # --- Fees are valid floats now --- 
                assert isinstance(buy_fee_rate, float) and isinstance(sell_fee_rate, float)

                total_fee_amount = (buy_fee_rate * ask_price + sell_fee_rate * bid_price) * volume
                pnl = (bid_price - ask_price) * volume - total_fee_amount

                if pnl > best_pnl:
                    best_pnl = pnl
                    best = {
                        'pair':       pair,
                        'buy_ex':     buy_ex,
                        'sell_ex':    sell_ex,
                        'ask_price':  ask_price,
                        'bid_price':  bid_price,
                        'volume':     volume,
                        'buy_fee':    buy_fee_rate * ask_price * volume, # Use retrieved rate
                        'sell_fee':   sell_fee_rate * bid_price * volume # Use retrieved rate
                    }
    trades: List[Trade] = []
    if best:
        trades.append(Trade({
            'pair':      best['pair'],
            'venue':     best['buy_ex'],
            'side':      'buy',
            'price':     best['ask_price'],
            'volume':    best['volume'],
            'fee':       best['buy_fee'],
            'timestamp': None
        }))
        trades.append(Trade({
            'pair':      best['pair'],
            'venue':     best['sell_ex'],
            'side':      'sell',
            'price':     best['bid_price'],
            'volume':    best['volume'],
            'fee':       best['sell_fee'],
            'timestamp': None
        }))
    return trades


def triangle_arbitrage(state: MarketState) -> List[Trade]:
    """
    Triangular arbitrage on a single exchange: evaluates both cycles:
      A) USDC->BTC->ETH->USDC
      B) USDC->ETH->BTC->USDC
    Returns the most profitable 3-leg sequence, or an empty list.
    """
    best_trades: List[Trade] = []
    best_pnl = 0.0
    volume_scale = 1.0

    # Define pair strings
    p_btc_usdc = 'BTC/USDC'
    p_eth_btc  = 'ETH/BTC'
    p_eth_usdc = 'ETH/USDC'

    for ex, tickers in state.items():
        # --- Get exchange-specific fee dictionary ---
        ex_fees = FEES.get(ex)
        if ex_fees is None: continue

        # --- Get pair-specific fees for the triangle ---
        fee_btc_usdc = ex_fees.get(p_btc_usdc)
        fee_eth_btc  = ex_fees.get(p_eth_btc)
        fee_eth_usdc = ex_fees.get(p_eth_usdc)

        # --- Check if all required fees exist ---
        if None in (fee_btc_usdc, fee_eth_btc, fee_eth_usdc): continue
        assert isinstance(fee_btc_usdc, float) and isinstance(fee_eth_btc, float) and isinstance(fee_eth_usdc, float)

        # --- Ensure all tickers exist for the triangle ---
        ticker_btc_usdc = tickers.get(p_btc_usdc)
        ticker_eth_btc  = tickers.get(p_eth_btc)
        ticker_eth_usdc = tickers.get(p_eth_usdc)
        if not all((ticker_btc_usdc, ticker_eth_btc, ticker_eth_usdc)):
            continue
        # --- Assert tickers are not None --- 
        assert ticker_btc_usdc is not None
        assert ticker_eth_btc is not None
        assert ticker_eth_usdc is not None
        # --- --- --- 

        # --- Extract book quotes --- 
        ask_btc_usdc, sz_ask_btc_usdc = ticker_btc_usdc.get('ask'), ticker_btc_usdc.get('ask_size')
        bid_btc_usdc, sz_bid_btc_usdc = ticker_btc_usdc.get('bid'), ticker_btc_usdc.get('bid_size')
        ask_eth_btc,  sz_ask_eth_btc  = ticker_eth_btc.get('ask'), ticker_eth_btc.get('ask_size')
        bid_eth_btc,  sz_bid_eth_btc  = ticker_eth_btc.get('bid'), ticker_eth_btc.get('bid_size')
        ask_eth_usdc, sz_ask_eth_usdc = ticker_eth_usdc.get('ask'), ticker_eth_usdc.get('ask_size')
        bid_eth_usdc, sz_bid_eth_usdc = ticker_eth_usdc.get('bid'), ticker_eth_usdc.get('bid_size')

        # --- Check required values and assert types --- 
        required_values = (
            ask_btc_usdc, sz_ask_btc_usdc, bid_btc_usdc, sz_bid_btc_usdc,
            ask_eth_btc,  sz_ask_eth_btc,  bid_eth_btc,  sz_bid_eth_btc,
            ask_eth_usdc, sz_ask_eth_usdc, bid_eth_usdc, sz_bid_eth_usdc
        )
        if None in required_values: continue
        
        assert ask_btc_usdc is not None and sz_ask_btc_usdc is not None
        assert bid_btc_usdc is not None and sz_bid_btc_usdc is not None
        assert ask_eth_btc is not None and sz_ask_eth_btc is not None
        assert bid_eth_btc is not None and sz_bid_eth_btc is not None
        assert ask_eth_usdc is not None and sz_ask_eth_usdc is not None
        assert bid_eth_usdc is not None and sz_bid_eth_usdc is not None
        # --- --- --- 

        # === Cycle A: USDC -> BTC -> ETH -> USDC ===
        # Max BTC volume based on BTC/USDC ask size
        max_vol_btc_A = sz_ask_btc_usdc 
        if ask_eth_btc == 0: continue # Avoid division by zero
        # Max ETH volume based on BTC volume and ETH/BTC ask price
        max_vol_eth_A = max_vol_btc_A / ask_eth_btc
        # Limit ETH volume by ETH/USDC bid size
        max_vol_eth_A = min(max_vol_eth_A, sz_bid_eth_usdc)
        if max_vol_eth_A <= 0: continue

        # Final volumes based on limits
        final_vol_eth_A = max_vol_eth_A
        final_vol_btc_A = final_vol_eth_A * ask_eth_btc
        
        # Cost and proceeds
        usdc_in_A  = final_vol_btc_A * ask_btc_usdc # Cost to buy BTC
        usdc_out_A = final_vol_eth_A * bid_eth_usdc # Proceeds from selling ETH
        pnl_A = usdc_out_A - usdc_in_A

        # Fees for Cycle A (approximate for ETH/BTC leg)
        fee_A_leg1 = usdc_in_A * fee_btc_usdc   # Buy BTC/USDC fee (USDC)
        fee_A_leg2 = (final_vol_eth_A * ask_eth_btc) * fee_eth_btc # Buy ETH/BTC fee (BTC value)
        fee_A_leg3 = usdc_out_A * fee_eth_usdc  # Sell ETH/USDC fee (USDC)
        total_fee_A = fee_A_leg1 + (fee_A_leg2 * ask_btc_usdc) + fee_A_leg3 # Approx total fee in USDC
        net_pnl_A = pnl_A - total_fee_A

        # === Cycle B: USDC -> ETH -> BTC -> USDC ===
        # Max ETH volume based on ETH/USDC ask size
        max_vol_eth_B = sz_ask_eth_usdc
        if bid_eth_btc == 0: continue # Avoid division by zero
        # Max BTC volume based on ETH volume and ETH/BTC bid price
        max_vol_btc_B = max_vol_eth_B * bid_eth_btc
        # Limit BTC volume by BTC/USDC bid size
        max_vol_btc_B = min(max_vol_btc_B, sz_bid_btc_usdc)
        if max_vol_btc_B <= 0: continue
        
        # Final volumes based on limits
        final_vol_btc_B = max_vol_btc_B
        final_vol_eth_B = final_vol_btc_B / bid_eth_btc
        if final_vol_eth_B <= 0: continue

        # Cost and proceeds
        usdc_in_B = final_vol_eth_B * ask_eth_usdc # Cost to buy ETH
        usdc_out_B = final_vol_btc_B * bid_btc_usdc # Proceeds from selling BTC
        pnl_B = usdc_out_B - usdc_in_B

        # Fees for Cycle B (approximate for ETH/BTC leg)
        fee_B_leg1 = usdc_in_B * fee_eth_usdc   # Buy ETH/USDC fee (USDC)
        fee_B_leg2 = final_vol_btc_B * fee_eth_btc    # Sell ETH/BTC fee (BTC value)
        fee_B_leg3 = usdc_out_B * fee_btc_usdc  # Sell BTC/USDC fee (USDC)
        total_fee_B = fee_B_leg1 + (fee_B_leg2 * bid_btc_usdc) + fee_B_leg3 # Approx total fee in USDC
        net_pnl_B = pnl_B - total_fee_B

        # === Choose best cycle ===
        if net_pnl_A > best_pnl and net_pnl_A > net_pnl_B:
            best_pnl = net_pnl_A
            best_trades = [
                # Leg 1: Buy BTC/USDC
                Trade({'pair': p_btc_usdc, 'venue': ex, 'side': 'buy',  'price': ask_btc_usdc, 'volume': final_vol_btc_A, 'fee': fee_btc_usdc*ask_btc_usdc*final_vol_btc_A, 'timestamp': None}),
                # Leg 2: Buy ETH/BTC
                Trade({'pair': p_eth_btc,  'venue': ex, 'side': 'buy',  'price': ask_eth_btc,  'volume': final_vol_eth_A, 'fee': fee_eth_btc *ask_eth_btc *final_vol_eth_A, 'timestamp': None}),
                # Leg 3: Sell ETH/USDC
                Trade({'pair': p_eth_usdc, 'venue': ex, 'side': 'sell', 'price': bid_eth_usdc, 'volume': final_vol_eth_A, 'fee': fee_eth_usdc*bid_eth_usdc*final_vol_eth_A, 'timestamp': None})
            ]
        elif net_pnl_B > best_pnl:
            best_pnl = net_pnl_B
            best_trades = [
                # Leg 1: Buy ETH/USDC
                Trade({'pair': p_eth_usdc, 'venue': ex, 'side': 'buy',  'price': ask_eth_usdc, 'volume': final_vol_eth_B, 'fee': fee_eth_usdc*ask_eth_usdc*final_vol_eth_B, 'timestamp': None}),
                # Leg 2: Sell ETH/BTC
                Trade({'pair': p_eth_btc,  'venue': ex, 'side': 'sell', 'price': bid_eth_btc,  'volume': final_vol_eth_B, 'fee': fee_eth_btc *bid_eth_btc *final_vol_eth_B, 'timestamp': None}),
                # Leg 3: Sell BTC/USDC
                Trade({'pair': p_btc_usdc, 'venue': ex, 'side': 'sell', 'price': bid_btc_usdc, 'volume': final_vol_btc_B, 'fee': fee_btc_usdc*bid_btc_usdc*final_vol_btc_B, 'timestamp': None})
            ]
            
    return best_trades
