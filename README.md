# Bybit Klines Fetcher

This repository contains a Python script to fetch candlestick (kline) data from the Bybit API and save it to a CSV file.

## Usage

1. Install the required packages:
    ```bash
    pip install pybit pandas
    ```

2. Run the script:
    ```bash
    python fetch_klines_data.py
    ```

## Configuration

Modify the `symbol`, `interval`, `start_date`, and `end_date` variables in the `fetch_klines_data.py` script to fetch the desired data.

## Features

- **Easy Date Range Specification**: Allows you to easily specify the start and end dates for the data you want to fetch.
- **Large Dataset Handling**: Handles pagination to retrieve large datasets efficiently.
- **Data Quality Checks**: Checks for duplicate timestamps and irregular intervals in the fetched data.
- **CSV Export**: Saves the fetched data to a CSV file for further analysis.

## Example

Example of fetching 1-hour candlestick data for XRPUSDT from 2018-01-01 to 2025-03-28:
```python
symbol = "XRPUSDT"
interval = '60'  # 1-hour candlestick
start_date = "2018-01-01"
end_date = "2025-03-28"
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
