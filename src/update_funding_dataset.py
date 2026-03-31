"""
Binance Funding Rate Updater

An automated data pipeline script designed to fetch and maintain historical
funding rate data from Binance USD-M and COIN-M futures markets.
"""

import argparse
import json
import time
import os
from pathlib import Path

import ccxt
import pandas as pd


LIMIT = 1000  # Maximum number of funding rates to fetch in each API call
MAX_RETRIES = 5  # Maximum number of retries
SLEEP_SECONDS = 7  # Sleep time in between API calls

MARKETS = {
    "um": {
        "cls": ccxt.binanceusdm,
        "filter": lambda m: m.get("swap") and m.get("linear"),
        "start": "2019-09-01T00:00:00Z",
    },
    "cm": {
        "cls": ccxt.binancecoinm,
        "filter": lambda m: m.get("swap") and m.get("inverse"),
        "start": "2020-01-01T00:00:00Z",
    },
}


def fetch_history(exchange: ccxt.Exchange, symbol: str, since_ms: int) -> list[dict]:
    """
    Fetches historical funding rate data for a single symbol using pagination.

    Args:
        exchange (ccxt.Exchange): The instantiated CCXT exchange object.
        symbol (str): The CCXT-formatted unified symbol (e.g., 'BTC/USDT:USDT').
        since_ms (int): The starting timestamp in milliseconds.

    Returns:
        list[dict]: A list of dictionaries containing raw funding rate records.

    Raises:
        RuntimeError: If the maximum number of network/exchange retries is exceeded.
    """
    rows = []
    while True:
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                batch = exchange.fetch_funding_rate_history(
                    symbol, since=since_ms, limit=LIMIT
                )
                break
            except Exception as e:
                last_error = e
                print(
                    f"\n[Attempt {attempt + 1}/{MAX_RETRIES}] {type(e).__name__} on {symbol}: {str(e)}"
                )
                time.sleep(min(5 * (attempt + 1), 30))
        else:
            raise RuntimeError(
                f"Failed fetching {symbol} after {MAX_RETRIES} retries. Last Error: {str(last_error)}"
            )

        if not batch:
            break

        rows.extend(batch)
        since_ms = int(batch[-1]["timestamp"]) + 1

        if len(batch) < LIMIT:
            break

    return rows


def process_market(root: Path, market_type: str, state: dict) -> dict:
    """
    Sweeps all symbols for a specific market type and updates local Parquet storage.

    Args:
        root (Path): The root directory of the Kaggle dataset.
        market_type (str): The market identifier ('um' for USD-M, 'cm' for COIN-M).
        state (dict): The current checkpoint state containing latest timestamps per symbol.

    Returns:
        dict: The updated checkpoint state dictionary.
    """
    cfg = MARKETS[market_type]

    exchange_config = {"enableRateLimit": True}

    proxy_url = os.environ.get("HTTP_PROXY")
    if proxy_url:
        exchange_config["proxies"] = {
            "http": proxy_url,
            "https": proxy_url,
        }
        print(f"Routing traffic through: {proxy_url}")

    exchange = cfg["cls"](exchange_config)
    start_ms = exchange.parse8601(cfg["start"])

    symbols = [s for s, m in exchange.load_markets().items() if cfg["filter"](m)]
    print(f"[{market_type.upper()}] Found {len(symbols)} swap markets.")

    new_data = []
    for ccxt_symbol in symbols:
        market_id = exchange.markets[ccxt_symbol]["id"]

        # Resume from checkpoint or start from market inception
        latest_ts = state.get(market_type, {}).get(market_id, start_ms)
        since_ms = latest_ts

        rows = fetch_history(exchange, ccxt_symbol, since_ms)
        time.sleep(SLEEP_SECONDS)

        if not rows:
            continue

        df = pd.DataFrame(
            [
                {
                    "market_type": market_type,
                    "symbol": market_id,
                    "timestamp": r["timestamp"],
                    "datetime": pd.to_datetime(r["timestamp"], unit="ms", utc=True),
                    "fundingRate": r.get("fundingRate"),
                }
                for r in rows
            ]
        ).dropna(subset=["timestamp"])

        if df.empty:
            continue

        # Strict PyArrow schema enforcement
        df["timestamp"] = df["timestamp"].astype("int64")
        df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
        df = df.dropna(subset=["fundingRate"])

        df = df.drop_duplicates(subset=["symbol", "timestamp"], keep="last")
        new_data.append(df)

        state.setdefault(market_type, {})[market_id] = int(df["timestamp"].max())
        print(f"  -> {market_id}: Fetched {len(df)} rows")

    if new_data:
        df_new = pd.concat(new_data, ignore_index=True)

        out_file = root / f"{market_type}_funding_rates.parquet"
        if out_file.exists():
            df_old = pd.read_parquet(out_file)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_all = df_new

        df_all = df_all.drop_duplicates(
            ["symbol", "timestamp"], keep="last"
        ).sort_values(["symbol", "timestamp"])

        # Memory optimization before saving
        df_all["market_type"] = df_all["market_type"].astype("category")
        df_all["symbol"] = df_all["symbol"].astype("category")

        df_all.to_parquet(out_file, index=False)

    return state


def main():
    """
    Entry point for the CI/CD pipeline. Parses arguments and sequentially processes markets.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--only-market", choices=["um", "cm", "both"], default="both")
    args = parser.parse_args()

    root = Path(args.dataset_root)
    state_file = root / "state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)

    state = json.loads(state_file.read_text()) if state_file.exists() else {}
    markets = ["um", "cm"] if args.only_market == "both" else [args.only_market]

    for m in markets:
        state = process_market(root, m, state)

    state_file.write_text(json.dumps(state, indent=2))
    print("Run complete.")


if __name__ == "__main__":
    main()
