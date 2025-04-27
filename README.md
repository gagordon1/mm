# Crypto Arbitrage Data Collector & Backtester

This project consists of two main Python scripts:

1.  `collector.py`: Connects to multiple cryptocurrency exchanges using `ccxt.pro` and WebSockets to stream real-time Best Bid/Offer (BBO) data for a specified trading pair (provided via command-line arguments). It logs this data into daily Parquet files, separated by exchange.
2.  `backtest.py`: Reads the collected Parquet data for a specified trading pair (provided via command-line arguments), simulates a simple cross-exchange arbitrage strategy based on BBO, calculates potential Profit and Loss (PnL) considering fees and simulated order book impact, and plots the cumulative PnL over time.

## Setup

1.  **Clone the Repository:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```

2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate # On Windows use `venv\Scripts\activate`
    ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configuration:**
    *   **Trading Pair:** The trading pair (e.g., BTC/USDC) is now specified using the `--coin` and `--base` arguments when running the scripts (see Usage section).
    *   **Data Directory:** The data directory is automatically determined based on the command-line arguments (e.g., `--coin BTC --base USDC` will use `data/BTC-USDC/`).
    *   **Exchanges:** Modify the `EXCHANGES` list within `collector.py` and `backtest.py` if you want to add/remove exchanges. Ensure the exchange IDs are valid according to `ccxt.pro` or handled specifically (like Hyperliquid).
    *   **Fees:** Adjust the `FEES` dictionary in `backtest.py` to reflect the maker fees for the exchanges you are using. Accurate fees are crucial for realistic backtesting.

## Usage

1.  **Run the Data Collector:**
    Open a terminal, activate your virtual environment, and run the collector script specifying the coin and base currency:
    ```bash
    python collector.py --coin BTC --base USDC
    # Example for ETH/USDC:
    # python collector.py --coin ETH --base USDC
    ```
    *   Replace `BTC` and `USDC` with the desired trading pair components.
    *   This will start streaming BBO data for the specified pair from the configured `EXCHANGES`.
    *   It will print live BBO updates to the console.
    *   Data will be saved in Parquet files within the corresponding `data/{COIN}-{BASE}/` directory (e.g., `data/BTC-USDC/coinbase_YYYY-MM-DD.parquet`).
    *   Let it run for a sufficient period to collect data. Press `Ctrl+C` to stop the collector gracefully (it will flush any remaining buffered data).

2.  **Run the Backtester:**
    Once you have collected data, open *another* terminal (or stop the collector), activate the virtual environment, and run the backtester, specifying the *same* coin and base currency used for collection:
    ```bash
    python backtest.py --coin BTC --base USDC
    # Example for ETH/USDC:
    # python backtest.py --coin ETH --base USDC
    ```
    *   The script will load all relevant Parquet files from the corresponding `data/{COIN}-{BASE}/` directory.
    *   It will process the historical events and simulate the arbitrage strategy.
    *   Trades executed during the backtest will be printed to the console.
    *   Finally, it will print the total number of trades and the total PnL.
    *   If profitable trades were found, a plot showing the cumulative PnL over time will be displayed.

## Notes

*   The backtester's book impact simulation currently assumes *any* trade clears the top-of-book level on the involved exchanges (`state[exch]['bid/ask'] = None`). This is a conservative simplification.
*   Timestamps are based on the collector machine's clock (`time.time_ns()`) at the moment data is processed, not the exchange's event timestamp.
*   Ensure you have sufficient disk space, as raw BBO data can accumulate quickly. 