import pandas as pd


def is_high_volume(df: pd.DataFrame, window: int = 100, z_thresh: float = 1.5) -> pd.Series:
    vol = df["Volume"]
    vol_mean = vol.rolling(window).mean()
    vol_std = vol.rolling(window).std() + 1e-9
    z_score = (vol - vol_mean) / vol_std
    return z_score > z_thresh


def detect_hammer(
    df: pd.DataFrame, body_ratio_thresh: float = 0.5, lower_tail_ratio_thresh: float = 2.0
) -> pd.Series:
    body = (df["Close"] - df["Open"]).abs()
    range_ = df["High"] - df["Low"] + 1e-9
    lower_tail = df[["Open", "Close"]].min(axis=1) - df["Low"]
    upper_tail = df["High"] - df[["Open", "Close"]].max(axis=1)

    body_ratio = body / range_
    lower_tail_ratio = lower_tail / (body + 1e-9)
    upper_tail_ratio = upper_tail / (body + 1e-9)

    is_hammer = (
        (body_ratio < body_ratio_thresh)
        & (lower_tail_ratio > lower_tail_ratio_thresh)
        & (lower_tail > upper_tail)
        & (upper_tail_ratio < 0.5)
    )
    return is_hammer & is_high_volume(df)


def detect_bullish_engulfing(df: pd.DataFrame) -> pd.Series:
    prev_open = df["Open"].shift(1)
    prev_close = df["Close"].shift(1)
    prev_bearish = prev_close < prev_open
    current_bullish = df["Close"] > df["Open"]

    engulfing = (df["Open"] < prev_close) & (df["Close"] > prev_open)
    return prev_bearish & current_bullish & engulfing & is_high_volume(df)


def detect_bearish_engulfing(df: pd.DataFrame) -> pd.Series:
    prev_open = df["Open"].shift(1)
    prev_close = df["Close"].shift(1)
    prev_bullish = prev_close > prev_open
    current_bearish = df["Close"] < df["Open"]

    engulfing = (df["Open"] > prev_close) & (df["Close"] < prev_open)
    return prev_bullish & current_bearish & engulfing & is_high_volume(df)


def detect_morning_star(df: pd.DataFrame) -> pd.Series:
    o1, c1 = df["Open"].shift(2), df["Close"].shift(2)
    o2, c2 = df["Open"].shift(1), df["Close"].shift(1)
    o3, c3 = df["Open"], df["Close"]

    first_bearish = c1 < o1
    second_small = (c2 - o2).abs() / (df["High"].shift(1) - df["Low"].shift(1) + 1e-9) < 0.3
    third_bullish = c3 > o3
    recovery = c3 > ((o1 + c1) / 2)

    return first_bearish & second_small & third_bullish & recovery & is_high_volume(df)


def detect_shooting_star(
    df: pd.DataFrame, body_ratio_thresh: float = 0.25, tail_ratio_thresh: float = 2.0
) -> pd.Series:
    body = (df["Close"] - df["Open"]).abs()
    range_ = df["High"] - df["Low"] + 1e-9
    upper_tail = df["High"] - df[["Open", "Close"]].max(axis=1)
    lower_tail = df[["Open", "Close"]].min(axis=1) - df["Low"]

    body_ratio = body / range_
    tail_ratio = upper_tail / (body + 1e-9)

    is_star = (
        (body_ratio < body_ratio_thresh)
        & (tail_ratio > tail_ratio_thresh)
        & (upper_tail > lower_tail)
    )
    return is_star & is_high_volume(df)
