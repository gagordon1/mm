import requests

URL = "https://api.hyperliquid.xyz/info"
resp = requests.post(URL, json={"type": "spotMetaAndAssetCtxs"})
resp.raise_for_status()

data = resp.json()
# data is a list: [ spot_meta_dict, asset_contexts_list ]
spot_meta, _asset_contexts = data

# spot_meta["tokens"] is a list of dicts like:
#   { "name": "USDC", "szDecimals": ..., "weiDecimals": ..., "index": 0, "tokenId": "0x...", ... }
token_list = spot_meta["tokens"]
id2sym = { t["index"]: t["name"] for t in token_list }

# now loop your universe
for m in spot_meta["universe"]:
    idx         = m["index"]
    placeholder = m["name"]            # "@1", "@2", etc.
    base_id, quote_id = m["tokens"]    # e.g. [2,0]
    base_sym  = id2sym[base_id]
    quote_sym = id2sym[quote_id]
    print(f"{idx:>3} → {placeholder:>4}   {base_sym}/{quote_sym}")
