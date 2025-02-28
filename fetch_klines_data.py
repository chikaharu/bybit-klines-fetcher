from pybit.unified_trading import HTTP
from datetime import datetime, timezone
import pandas as pd
import time


def date_to_unix_ms(date_str: str) -> int:
    """
    Converts a date string (e.g., "2023-04-01 15:30:00") interpreted as UTC
    to a UNIX timestamp in milliseconds.
    If the time information is omitted, it is considered as "00:00:00".
    """
    if len(date_str) == 10:
        date_str += " 00:00:00"
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_all_klines(symbol: str, interval: int, start_date: str, end_date: str,
                     category: str = "linear", testnet: bool = False, limit: int = 1000):
    """
    Retrieves candlestick data for the specified period (start_date to end_date)
    from the Bybit API in batches of 'limit' records each, in descending order (latest to oldest).

    Pagination algorithm:
      1. Set desired_start to the user-specified start_date and desired_end to end_date (both in UTC UNIX milliseconds).
      2. Initialize query_end with the desired_end value.
      3. While query_end > desired_start, call the API.
         Since the API returns data in descending order, the last row of the response is the oldest candle.
      4. For subsequent requests, update query_end to the timestamp of the last row of the previous response minus 1.
    """
    session = HTTP(testnet=testnet)
    desired_start = date_to_unix_ms(start_date)
    desired_end = date_to_unix_ms(end_date)
    all_data = []
    query_end = desired_end

    while query_end > desired_start:
        print(f"Fetching data with query_end = {datetime.utcfromtimestamp(query_end / 1000)} UTC")
        response = session.get_kline(
            category=category,
            symbol=symbol,
            interval=interval,
            start=desired_start,  # Start date is fixed
            end=query_end,        # End date for the current query (latest side)
            limit=limit
        )
        data = response.get("result", {}).get("list", [])
        if not data:
            print("No more data returned.")
            break

        # The API returns data in descending order (latest to oldest)
        all_data.extend(data)

        # Get the oldest timestamp from the last retrieved batch
        oldest_ts = int(data[-1][0])
        if oldest_ts <= desired_start:
            # End condition reached
            break
        # Update query_end for the next batch (subtract 1 to avoid duplication)
        query_end = oldest_ts - 1
        time.sleep(0.002)  # Rate-limit protection

    return all_data


if __name__ == "__main__":
    # Example: Retrieve 1-hour candlestick data for XRPUSDT
    # from "2018-01-01" (desired_start) to "2025-03-28" (desired_end)
    symbol = "XRPUSDT"
    interval = '60'  # 1-hour candlestick
    start_date = "2018-01-01"
    end_date = "2025-03-28"

    data_list = fetch_all_klines(symbol, interval, start_date, end_date, category="linear", testnet=False, limit=1000)

    # Convert to DataFrame
    columns = ["timestamp", "open", "high", "low", "close", "volume", "turnover"]
    df = pd.DataFrame(data_list, columns=columns)

    # Convert the retrieved timestamp from string to int and sort.
    # The API returns data in descending order, so sort in ascending order (oldest first).
    df["timestamp"] = df["timestamp"].astype(int)
    df = df.sort_values("timestamp")

    # Finally, convert the timestamp to UTC datetime and format as "YYYY-MM-DD HH:MM" (omit seconds)
    df["timestamp"] = pd.to_datetime(df["timestamp"] / 1000, unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M")
    df.set_index("timestamp", inplace=True)

    # Check for duplicate timestamps
    duplicate_count = df.index.duplicated().sum()
    if duplicate_count > 0:
        print(f"There are {duplicate_count} duplicate entries.")
    else:
        print("No duplicates found.")

    # Calculate the time differences between rows
    time_diffs = pd.to_datetime(df.index).to_series().diff().dropna()

    # Expected interval for 1-hour candles
    expected_interval = pd.Timedelta(hours=1)

    # Find mismatches in the intervals
    mismatches = time_diffs[time_diffs != expected_interval]
    if len(mismatches) > 0:
        print("Gaps or irregular intervals detected:")
        print(mismatches)
    else:
        print("All candlestick intervals are as expected.")

    # Display statistics of time differences
    print("Statistics of time differences:")
    print(time_diffs.describe())

    print("First data (oldest record):")
    print(df.head())
    print("Last data (latest record):")
    print(df.tail())
    
    df.to_csv(f'{symbol}_{interval}_Lasted_bybit.csv')
    print('Saved to CSV :: ' + f'{symbol}_{interval}_Lasted_bybit.csv')
