from typing import Dict, Optional, TypedDict, List
from control import FEES, SYMBOL_MAP
from backtest import State, Trade


def cash_and_carry(state: State) -> List[Trade]:
    """
    Cash-and-carry strategy on Hyperliquid perp vs. spot on other venues.
    - If perp_bid > spot_ask: buy spot / sell perp
    - If perp_ask < spot_bid: sell spot / buy perp
    Earns positive basis and captures funding (funding_rate can be read from the perp Ticker).
    """
    print(state)
    print("--------------------------------")
    
    books     = state.tickers
    positions = state.positions
    trades: List[Trade] = []

    spot_pair = 'BTC/USDC'
    hyper     = 'hyperliquid-perp'
    perp_pair = SYMBOL_MAP['BTC'][hyper]

    # avoid re-entry if we already have ANY open perp position
    open_perp = positions.get(hyper, {}).get(perp_pair, 0)
    if open_perp != 0:
        return trades

    # --- find best spot ask and best spot bid across venues ---
    best_ask = float('inf'); ask_venue = None;  ask_sz = 0.0
    best_bid = -float('inf'); bid_venue = None; bid_sz = 0.0

    for venue, pairs in books.items():
        if venue == hyper:
            continue
        t = pairs.get(spot_pair)
        if not t:
            continue
        a, asz = t.get('ask'), t.get('ask_size')
        b, bsz = t.get('bid'), t.get('bid_size')
        if a and asz and asz>0 and a<best_ask:
            best_ask, ask_venue, ask_sz = a, venue, asz
        if b and bsz and bsz>0 and b>best_bid:
            best_bid, bid_venue, bid_sz = b, venue, bsz

    # no valid spot quotes?
    if ask_venue is None or bid_venue is None:
        return trades

    # --- perp top‐of‐book + funding_rate ---
    perp_t = books.get(hyper, {}).get(perp_pair)
    if not perp_t:
        return trades
    perp_bid    = perp_t.get('bid');    perp_bid_sz = perp_t.get('bid_size')
    perp_ask    = perp_t.get('ask');    perp_ask_sz = perp_t.get('ask_size')
    funding_pct = perp_t.get('funding_rate') or 0.0

    # Entry #1: perp > spot (buy spot, sell perp)
    if perp_bid and perp_bid_sz and perp_bid > best_ask:
        vol = min(ask_sz, perp_bid_sz)
        fee_spot_rate = FEES.get(ask_venue, {}).get(spot_pair, 0.0)
        fee_perp_rate = FEES.get(hyper, {}).get(perp_pair, 0.0)
        fee_spot = fee_spot_rate * best_ask * vol
        fee_perp = fee_perp_rate * perp_bid * vol
        # include one period of funding income (you're short perp, funding_pct>0 pays shorts)
        funding_income = funding_pct * perp_bid * vol
        pnl = (perp_bid - best_ask)*vol - (fee_spot+fee_perp) + funding_income
        
        if pnl > 0:
            trades.append(Trade({
                'pair':   spot_pair,
                'venue':  ask_venue,
                'side':   'buy',
                'price':  best_ask,
                'volume': vol,
                'fee':    fee_spot,
                'ts_ns':  None,
                'type':   'spot'
            }))
            trades.append(Trade({
                'pair':   perp_pair,
                'venue':  hyper,
                'side':   'sell',
                'price':  perp_bid,
                'volume': vol,
                'fee':    fee_perp,
                'ts_ns':  None,
                'type':   'perp'
            }))
            return trades

    # Entry #2: perp < spot (sell spot, buy perp)
    if perp_ask and perp_ask_sz and perp_ask < best_bid:
        vol = min(bid_sz, perp_ask_sz)
        fee_spot_rate = FEES.get(bid_venue, {}).get(spot_pair, 0.0)
        fee_perp_rate = FEES.get(hyper, {}).get(perp_pair, 0.0)
        fee_spot = fee_spot_rate * best_bid * vol
        fee_perp = fee_perp_rate * perp_ask * vol
        # you're long perp, funding_pct>0 → you pay funding_pct; as long, funding_pct>0 means longs pay shorts
        funding_cost = funding_pct * perp_ask * vol
        pnl = (best_bid - perp_ask)*vol - (fee_spot+fee_perp) - funding_cost
        if pnl > 0:
            trades.append(Trade({
                'pair':   spot_pair,
                'venue':  bid_venue,
                'side':   'sell',
                'price':  best_bid,
                'volume': vol,
                'fee':    fee_spot,
                'ts_ns':  None,
                'type':   'spot'
            }))
            trades.append(Trade({
                'pair':   perp_pair,
                'venue':  hyper,
                'side':   'buy',
                'price':  perp_ask,
                'volume': vol,
                'fee':    fee_perp,
                'ts_ns':  None,
                'type':   'perp'
            }))
            return trades

    return trades

def cross_exchange_arbitrage(state: State) -> List[Trade]:
    """
    Finds the best cross-exchange arbitrage opportunity based on tickers.
    (Currently ignores positions in the decision logic).
    Returns two Trade objects (buy leg, sell leg) for the best trade.
    """
    best = None
    best_pnl = 0.0
    volume_scale = 1.0
    
    tickers = state.tickers  # Access tickers from state
    # positions = state.positions  # Access positions if needed

    for buy_ex, ex_tickers in tickers.items(): # Iterate through tickers dict
        for pair, buy_t in ex_tickers.items():
            ask_price = buy_t.get('ask'); ask_size = buy_t.get('ask_size')
            if ask_price is None or ask_size is None or ask_size <= 0:
                continue
            for sell_ex, sell_ex_tickers in tickers.items(): # Iterate through tickers dict
                if sell_ex == buy_ex:
                    continue
                sell_t = sell_ex_tickers.get(pair)
                if not sell_t:
                    continue
                bid_price = sell_t.get('bid'); bid_size = sell_t.get('bid_size')
                if bid_price is None or bid_size is None or bid_size <= 0:
                    continue

                volume = min(ask_size, bid_size) * volume_scale
                buy_fee_rate = FEES.get(buy_ex, {}).get(pair)
                sell_fee_rate = FEES.get(sell_ex, {}).get(pair)
                if buy_fee_rate is None or sell_fee_rate is None:
                    continue 
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
                        'buy_fee':    buy_fee_rate * ask_price * volume,
                        'sell_fee':   sell_fee_rate * bid_price * volume
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
            'ts_ns': None,
            'type': 'spot'
        }))
        trades.append(Trade({
            'pair':      best['pair'],
            'venue':     best['sell_ex'],
            'side':      'sell',
            'price':     best['bid_price'],
            'volume':    best['volume'],
            'fee':       best['sell_fee'],
            'ts_ns': None,
            'type': 'spot'
        }))
    return trades


def triangle_arbitrage(state: State) -> List[Trade]:
    """
    Triangular arbitrage based on tickers. (Ignores positions).
    """
    best_trades: List[Trade] = []
    best_pnl = 0.0
    volume_scale = 1.0

    p_btc_usdc = 'BTC/USDC'
    p_eth_btc  = 'ETH/BTC'
    p_eth_usdc = 'ETH/USDC'
    
    tickers_state = state.tickers  # Access tickers from state
    # positions = state.positions  # Access positions if needed

    for ex, ex_tickers in tickers_state.items(): # Iterate through tickers_state
        # --- Fee lookups remain the same ---
        ex_fees = FEES.get(ex)
        if ex_fees is None: continue
        fee_btc_usdc = ex_fees.get(p_btc_usdc)
        fee_eth_btc  = ex_fees.get(p_eth_btc)
        fee_eth_usdc = ex_fees.get(p_eth_usdc)
        if None in (fee_btc_usdc, fee_eth_btc, fee_eth_usdc): continue
        assert isinstance(fee_btc_usdc, float) and isinstance(fee_eth_btc, float) and isinstance(fee_eth_usdc, float)

        # --- Use ex_tickers for ticker lookups --- 
        ticker_btc_usdc = ex_tickers.get(p_btc_usdc)
        ticker_eth_btc  = ex_tickers.get(p_eth_btc)
        ticker_eth_usdc = ex_tickers.get(p_eth_usdc)
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

        # === Cycle A: USDC -> BTC -> ETH -> USDC ===
        max_vol_btc_A = sz_ask_btc_usdc 
        if ask_eth_btc == 0: continue
        max_vol_eth_A = max_vol_btc_A / ask_eth_btc
        max_vol_eth_A = min(max_vol_eth_A, sz_bid_eth_usdc)
        if max_vol_eth_A <= 0: continue
        final_vol_eth_A = max_vol_eth_A
        final_vol_btc_A = final_vol_eth_A * ask_eth_btc
        usdc_in_A  = final_vol_btc_A * ask_btc_usdc
        usdc_out_A = final_vol_eth_A * bid_eth_usdc
        pnl_A = usdc_out_A - usdc_in_A
        fee_A_leg1 = usdc_in_A * fee_btc_usdc
        fee_A_leg2 = (final_vol_eth_A * ask_eth_btc) * fee_eth_btc
        fee_A_leg3 = usdc_out_A * fee_eth_usdc
        total_fee_A = fee_A_leg1 + (fee_A_leg2 * ask_btc_usdc) + fee_A_leg3
        net_pnl_A = pnl_A - total_fee_A

        # === Cycle B: USDC -> ETH -> BTC -> USDC ===
        max_vol_eth_B = sz_ask_eth_usdc
        if bid_eth_btc == 0: continue
        max_vol_btc_B = max_vol_eth_B * bid_eth_btc
        max_vol_btc_B = min(max_vol_btc_B, sz_bid_btc_usdc)
        if max_vol_btc_B <= 0: continue
        final_vol_btc_B = max_vol_btc_B
        final_vol_eth_B = final_vol_btc_B / bid_eth_btc
        if final_vol_eth_B <= 0: continue
        usdc_in_B = final_vol_eth_B * ask_eth_usdc
        usdc_out_B = final_vol_btc_B * bid_btc_usdc
        pnl_B = usdc_out_B - usdc_in_B
        fee_B_leg1 = usdc_in_B * fee_eth_usdc
        fee_B_leg2 = final_vol_btc_B * fee_eth_btc
        fee_B_leg3 = usdc_out_B * fee_btc_usdc
        total_fee_B = fee_B_leg1 + (fee_B_leg2 * bid_btc_usdc) + fee_B_leg3
        net_pnl_B = pnl_B - total_fee_B

        # === Choose best cycle ===
        if net_pnl_A > best_pnl and net_pnl_A > net_pnl_B:
            best_pnl = net_pnl_A
            best_trades = [
                Trade({'pair': p_btc_usdc, 'venue': ex, 'side': 'buy',  'price': ask_btc_usdc, 'volume': final_vol_btc_A, 'fee': fee_btc_usdc*ask_btc_usdc*final_vol_btc_A, 'ts_ns': None, 'type': 'spot'}),
                Trade({'pair': p_eth_btc,  'venue': ex, 'side': 'buy',  'price': ask_eth_btc,  'volume': final_vol_eth_A, 'fee': fee_eth_btc *ask_eth_btc *final_vol_eth_A, 'ts_ns': None, 'type': 'spot'}),
                Trade({'pair': p_eth_usdc, 'venue': ex, 'side': 'sell', 'price': bid_eth_usdc, 'volume': final_vol_eth_A, 'fee': fee_eth_usdc*bid_eth_usdc*final_vol_eth_A, 'ts_ns': None, 'type': 'spot'})
            ]
        elif net_pnl_B > best_pnl:
            best_pnl = net_pnl_B
            best_trades = [
                Trade({'pair': p_eth_usdc, 'venue': ex, 'side': 'buy',  'price': ask_eth_usdc, 'volume': final_vol_eth_B, 'fee': fee_eth_usdc*ask_eth_usdc*final_vol_eth_B, 'ts_ns': None, 'type': 'spot'}),
                Trade({'pair': p_eth_btc,  'venue': ex, 'side': 'sell', 'price': bid_eth_btc,  'volume': final_vol_eth_B, 'fee': fee_eth_btc *bid_eth_btc *final_vol_eth_B, 'ts_ns': None, 'type': 'spot'}),
                Trade({'pair': p_btc_usdc, 'venue': ex, 'side': 'sell', 'price': bid_btc_usdc, 'volume': final_vol_btc_B, 'fee': fee_btc_usdc*bid_btc_usdc*final_vol_btc_B, 'ts_ns': None, 'type': 'spot'})
            ]
            
    return best_trades


