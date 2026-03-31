# Binance Historical Funding Rate Data Pipeline

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/haozhu18/binance-funding-rate-history/update_funding.yml?label=Action%20Status&logo=github)](https://github.com/haozhu18/binance-funding-rate-history/actions)
[![Kaggle Dataset](https://img.shields.io/badge/Kaggle-Open_Dataset-blue?&logo=kaggle)](https://www.kaggle.com/datasets/haozhu18/binance-funding-rate-history)

An automated data pipeline that extracts historical funding rates for all Binance USD-M and COIN-M perpetual futures, and publishes them regularly to Kaggle.

## Features

* **Automated**: Runs as a scheduled GitHub Actions cron job, handling incremental updates without manual intervention.
* **Stateful Execution**: Utilizes a `state.json` checkpointing system. It tracks the exact millisecond of the last fetched row for every symbol, reducing duplicate API calls.
* **Memory-Optimized Storage**: Data is typed and cast before storage. Target variables are enforced as `float64`, while repeating strings (`market_type`, `symbol`) are cast as PyArrow `category` types.
