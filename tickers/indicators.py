import pandas as pd


def calculate_sma(df: pd.DataFrame, span: int = 20) -> pd.DataFrame:
    df = df.copy()
    df[f"sma_{span}"] = df["Close"].rolling(window=span).mean()
    return df


def calculate_ema(df: pd.DataFrame, span: int = 20) -> pd.DataFrame:
    df = df.copy()
    df[f"ema_{span}"] = df["Close"].ewm(span=span, adjust=False).mean()
    return df


def calculate_rsi(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    df = df.copy()
    delta = df["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=window).mean()
    avg_loss = loss.rolling(window=window).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    df[f"rsi_{window}"] = 100 - (100 / (1 + rs))
    return df


def calculate_macd(
    df: pd.DataFrame, span_short: int = 12, span_long: int = 26, signal_span: int = 9
) -> pd.DataFrame:
    df = df.copy()
    ema_short = df["Close"].ewm(span=span_short, adjust=False).mean()
    ema_long = df["Close"].ewm(span=span_long, adjust=False).mean()
    df["macd_line"] = ema_short - ema_long
    df["macd_signal"] = df["macd_line"].ewm(span=signal_span, adjust=False).mean()
    df["macd_hist"] = df["macd_line"] - df["macd_signal"]
    return df


def add_key_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add commonly used technical indicators to the DataFrame."""
    df = df.copy()
    for span in [20, 50, 100, 200]:
        df = calculate_ema(df, span=span)
    for span in [20, 50, 100, 200]:
        df = calculate_sma(df, span=span)
    df = calculate_rsi(df, window=14)
    df = calculate_macd(df)
    return df
