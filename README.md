# Crypto Arbitrage Data Collector & Backtester

This project consists of two main Python scripts:

1.  `collector.py`: Connects to multiple cryptocurrency exchanges using `ccxt.pro` and WebSockets to stream real-time Best Bid/Offer (BBO) data for a specified trading pair. It logs this data into daily Parquet files, separated by exchange.
2.  `backtest.py`: Reads the collected Parquet data, simulates a simple cross-exchange arbitrage strategy based on BBO, calculates potential Profit and Loss (PnL) considering fees and simulated order book impact, and plots the cumulative PnL over time.

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
    *   **Symbol:** Ensure the `SYMBOL` variable (e.g., `'BTC/USD'`) and the corresponding `SUBPATH` (e.g., `'BTC-USD'`) are set **identically** in both `collector.py` and `backtest.py`. The backtester reads data from `data/{SUBPATH}/`.
    *   **Exchanges:** Modify the `EXCHANGES` list in both scripts if you want to add/remove exchanges. Ensure the exchange IDs are valid according to `ccxt.pro` or handled specifically (like Hyperliquid).
    *   **Fees:** Adjust the `FEES` dictionary in `backtest.py` to reflect the maker fees for the exchanges you are using. Accurate fees are crucial for realistic backtesting.

## Usage

1.  **Run the Data Collector:**
    Open a terminal, activate your virtual environment, and run:
    ```bash
    python collector.py
    ```
    *   This will start streaming BBO data for the configured `SYMBOL` from the specified `EXCHANGES`.
    *   It will print live BBO updates to the console.
    *   Data will be saved in Parquet files within the `data/{SUBPATH}/` directory (e.g., `data/BTC-USD/coinbase_2023-10-27.parquet`).
    *   Let it run for a sufficient period to collect data for backtesting. Press `Ctrl+C` to stop the collector gracefully (it will flush any remaining buffered data).

2.  **Run the Backtester:**
    Once you have collected some data, open *another* terminal (or stop the collector), activate the virtual environment, and run:
    ```bash
    python backtest.py
    ```
    *   The script will load all relevant Parquet files from the `data/{SUBPATH}/` directory for the configured `SYMBOL`.
    *   It will process the historical events and simulate the arbitrage strategy.
    *   Trades executed during the backtest will be printed to the console, showing details like buy/sell exchanges, prices, volume, fees, and PnL.
    *   Finally, it will print the total number of trades and the total PnL.
    *   If profitable trades were found, a plot showing the cumulative PnL over time will be displayed.

## Notes

*   The backtester's book impact simulation currently assumes *any* trade clears the top-of-book level on the involved exchanges (`state[exch]['bid/ask'] = None`). This is a conservative simplification.
*   Timestamps are based on the collector machine's clock (`time.time_ns()`) at the moment data is processed, not the exchange's event timestamp.
*   Ensure you have sufficient disk space, as raw BBO data can accumulate quickly. 