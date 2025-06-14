import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
from pandas.tseries.holiday import USFederalHolidayCalendar

from tickers.consts import *
from tickers.indicators import add_key_indicators
from tickers.utils import DirManager


def normalize_volume(df: pd.DataFrame, long_window=252, short_window=20) -> pd.DataFrame:
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df.sort_values("Date", inplace=True)
    df["weekday"] = df["Date"].dt.dayofweek

    # Long-term z-score of volume
    long_avg = df["Volume"].rolling(window=long_window, min_periods=long_window // 2).mean()
    long_std = df["Volume"].rolling(window=long_window, min_periods=long_window // 2).std()
    df["volume_zscore"] = (df["Volume"] - long_avg) / (long_std + 1e-9)

    # Weekday normalization
    weekday_avg_map = df.groupby("weekday")["Volume"].mean()
    df["volume_weekday_ratio"] = df["weekday"].map(weekday_avg_map)
    weekday_ratio = df["Volume"] / (df["volume_weekday_ratio"] + 1e-9)

    # Calendar effects
    df["is_opex"] = df["Date"].apply(lambda d: d.weekday() == 4 and 15 <= d.day <= 21)

    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=df["Date"].min(), end=df["Date"].max())
    df["is_holiday_adjacent"] = df["Date"].isin(holidays.shift(1, freq="B")) | df["Date"].isin(
        holidays.shift(-1, freq="B")
    )

    df["is_eoq"] = df["Date"].dt.is_quarter_end

    # Initialize scaling factor to 1.0
    scale = np.ones(len(df))
    scale[df["is_opex"]] *= 0.8  # downweight high-volume OPEX days
    scale[df["is_holiday_adjacent"]] *= 1.1  # boost suppressed volume near holidays
    scale[df["is_eoq"]] *= 0.9  # moderate EOQ volume

    # Final normalized volume feature
    normalized = (df["volume_zscore"] / (weekday_ratio + 1e-9)) * scale
    df["normalized_volume"] = normalized.values.ravel()
    df["normalized_volume"] = df["normalized_volume"].clip(-5, 5)

    return df.drop(columns=["weekday", "volume_zscore", "is_opex", "is_holiday_adjacent", "is_eoq"])


def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]
    df.rename(columns={"Symbol": "Ticker"}, inplace=True)
    df["Ticker"] = df["Ticker"].str.replace(".", "-", regex=False)  # Match Yahoo-style tickers
    return df[["Ticker", "Security", "GICS Sector", "GICS Sub-Industry"]]


def get_yahoo_metadata(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        return {
            "Ticker": ticker,
            "Name": info.get("shortName"),
            "Market Cap": info.get("marketCap"),
            "Sector": info.get("sector"),
            "Industry": info.get("industry"),
            "Full Time Employees": info.get("fullTimeEmployees"),
        }
    except Exception as e:
        return {"Ticker": ticker, "Error": str(e)}


def get_sp500_metadata(outdir="."):
    metadata_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {
            executor.submit(get_yahoo_metadata, ticker): ticker
            for ticker in get_sp500_tickers()["Ticker"]
        }
        for future in as_completed(future_to_ticker):
            metadata_results.append(future.result())
    metadata_df = pd.DataFrame(metadata_results)
    today_str = datetime.today().strftime("%Y-%m-%d")
    filename = f"sp500_yahoo_metadata_{today_str}.csv"
    with DirManager(outdir):
        metadata_df.to_csv(filename, index=False)
    return metadata_df


def get_csv_path(ticker: str) -> str:
    return os.path.join(DATA_DIR, f"{ticker}.csv")


def get_last_saved_date(ticker: str) -> pd.Timestamp:
    path = get_csv_path(ticker)
    if os.path.exists(path):
        df = pd.read_csv(path, parse_dates=["Date"])
        return df["Date"].max()
    else:
        return None


def fetch_new_data(ticker: str, start_date: str) -> pd.DataFrame:
    df = yf.download(ticker, start=start_date, progress=False, auto_adjust=True)
    if df.empty:
        return pd.DataFrame()
    else:
        df.columns = df.columns.droplevel(level=1)
        df.reset_index(inplace=True)
        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]
        df = add_key_indicators(df)
        return df


def update_ticker_data(ticker: str) -> pd.DataFrame:
    path = get_csv_path(ticker)
    last_date = get_last_saved_date(ticker)
    today = datetime.today().date()

    if last_date:
        start_date = (last_date + timedelta(days=1)).date()
        if start_date > today:
            # Data is already up to date
            return pd.read_csv(path, parse_dates=["Date"])
        start_date_str = start_date.strftime("%Y-%m-%d")
        new_data = fetch_new_data(ticker, start_date_str)
        if new_data.empty:
            return pd.read_csv(path, parse_dates=["Date"])
        existing = pd.read_csv(path, parse_dates=["Date"])
        combined = pd.concat([existing, new_data])
        combined.drop_duplicates(subset="Date", inplace=True)
    else:
        combined = fetch_new_data(ticker, start_date="1900-01-01")
        if combined.empty:
            return pd.DataFrame()

    combined.sort_values("Date", inplace=True)
    combined.to_csv(path, index=False)
    return combined


def batch_update_tickers_parallel(ticker_list: list, max_workers: int = 10) -> dict:
    all_data = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {
            executor.submit(update_ticker_data, ticker): ticker for ticker in ticker_list
        }
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                data = future.result()
                all_data[ticker] = data
                print(f"Processed {ticker}.")
            except Exception as e:
                print(f"Error processing {ticker}: {e}")
    return all_data
