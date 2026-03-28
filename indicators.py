import pandas as pd
import numpy as np

def calculate_rma(s, length):
    """
    Calculates the Running Moving Average (RMA) exactly as PineScript's ta.rma().

    PineScript formula:
        rma[0] = na (for first length-1 valid bars)
        rma[length-1] = sma(source, length)   <- seed with SMA
        rma[i] = 1/length * src[i] + (1 - 1/length) * rma[i-1]

    This explicit loop avoids any initialisation ambiguity in pandas ewm().
    Using ewm(alpha=alpha, adjust=False, min_periods=length) does NOT match
    TradingView because pandas seeds from bar 0, not bar length-1.
    """
    values = s.values.astype(float)
    n = len(values)
    out = np.full(n, np.nan)
    alpha = 1.0 / length

    # Find first non-NaN index
    seed_start = None
    for i in range(n):
        if not np.isnan(values[i]):
            seed_start = i
            break
    if seed_start is None:
        return pd.Series(out, index=s.index)

    seed_end = seed_start + length   # exclusive
    if seed_end > n:
        return pd.Series(out, index=s.index)

    # Seed = SMA of first `length` valid values (matches PineScript exactly)
    out[seed_end - 1] = float(np.nanmean(values[seed_start:seed_end]))

    for i in range(seed_end, n):
        if np.isnan(values[i]):
            out[i] = np.nan
        else:
            out[i] = alpha * values[i] + (1.0 - alpha) * out[i - 1]

    return pd.Series(out, index=s.index)


def calculate_ema(df, length):
    return df['close'].ewm(span=length, adjust=False, min_periods=length).mean()

def calculate_atr(df, length):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return calculate_rma(tr, length)

def calculate_bollinger_bands(df, length=20, std_dev=2.0):
    sma = df['close'].rolling(window=length).mean()
    # ddof=0 matches pandas_ta bbands defaults for population std dev
    std = df['close'].rolling(window=length).std(ddof=0)
    bbl = sma - (std_dev * std)
    bbu = sma + (std_dev * std)
    return bbl, bbu


def calculate_tv_rsi(df, length):
    """
    Calculates RSI exactly like TradingView/PineScript:

        up   = rma(max(change(close), 0), length)
        down = rma(-min(change(close), 0), length)
        rsi  = down == 0 ? 100 : up == 0 ? 0 : 100 - (100 / (1 + up/down))
    """
    delta = df['close'].diff()
    up   = delta.clip(lower=0)           # max(change, 0)
    down = (-delta).clip(lower=0)        # -min(change, 0)  <- equivalent

    rma_up   = calculate_rma(up,   length)
    rma_down = calculate_rma(down, length)

    # Build RSI following PineScript ternary exactly
    rsi = pd.Series(np.nan, index=df.index)
    for i in range(len(rsi)):
        u = rma_up.iloc[i]
        d = rma_down.iloc[i]
        if np.isnan(u) or np.isnan(d):
            rsi.iloc[i] = np.nan
        elif d == 0.0:
            rsi.iloc[i] = 100.0
        elif u == 0.0:
            rsi.iloc[i] = 0.0
        else:
            rsi.iloc[i] = 100.0 - (100.0 / (1.0 + u / d))

    return rsi

def calculate_rsi_divergence(df, rsi_fast_len=5, rsi_slow_len=14):
    """
    Calculates RSI Divergence exactly mathematically matching TradingView script by Shizaru:
    RSI Divergence = Fast RSI - Slow RSI
    Buy Signal: RSI Divergence crosses above 0
    Sell Signal: RSI Divergence crosses below 0
    """
    if df.empty or len(df) < rsi_slow_len * 2:
        return pd.DataFrame()

    # Calculate EMAs for Trend definition
    ema9 = calculate_ema(df, 9)
    ema14 = calculate_ema(df, 14)
    ema21 = calculate_ema(df, 21)

    trend_vals = []
    e9 = ema9.values if ema9 is not None else np.zeros(len(df))
    e14 = ema14.values if ema14 is not None else np.zeros(len(df))
    e21 = ema21.values if ema21 is not None else np.zeros(len(df))

    for i in range(len(df)):
        if e9[i] > e14[i] and e14[i] > e21[i]:
            trend_vals.append("Bullish")
        elif e9[i] < e14[i] and e14[i] < e21[i]:
            trend_vals.append("Bearish")
        else:
            trend_vals.append("Neutral")

    # Baseline indicators
    atr_14 = calculate_atr(df, 14)
    bbl, bbu = calculate_bollinger_bands(df, 20, 2.0)

    # RSI Calculations based perfectly on TradingView PineScript RMA math
    fast_rsi = calculate_tv_rsi(df, rsi_fast_len)
    slow_rsi = calculate_tv_rsi(df, rsi_slow_len)
    
    rsi_divergence = fast_rsi - slow_rsi

    # Signal Generation: Zero Crossover
    # 1 for Bullish crossover (from below 0 to above 0)
    # -1 for Bearish crossover (from above 0 to below 0)
    # 0 for no signal
    
    signals = np.zeros(len(df))
    rsi_div_vals = rsi_divergence.values
    
    for i in range(1, len(df)):
        if pd.isna(rsi_div_vals[i]) or pd.isna(rsi_div_vals[i-1]):
            continue
            
        # Buy Signal
        if rsi_div_vals[i] > 0 and rsi_div_vals[i-1] <= 0:
            signals[i] = 1
        # Sell Signal
        elif rsi_div_vals[i] < 0 and rsi_div_vals[i-1] >= 0:
            signals[i] = -1

    # Construct Result DataFrame
    result_df = df.copy()
    result_df['Signal'] = signals
    result_df['Signal_Price'] = df['close'] # Price at signal is the closing price
    result_df['Trend'] = trend_vals
    result_df['EMA9'] = ema9
    result_df['EMA14'] = ema14
    result_df['EMA21'] = ema21
    result_df['ATR'] = atr_14
    result_df['BBL'] = bbl
    result_df['BBU'] = bbu
    result_df['Fast_RSI'] = fast_rsi
    result_df['Slow_RSI'] = slow_rsi
    result_df['RSI_Divergence'] = rsi_divergence
    
    return result_df
