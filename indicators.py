import pandas as pd
import numpy as np

def calculate_rma(s, length):
    """
    Calculates the Running Moving Average (RMA) as used in PineScript.
    alpha = 1 / length
    rma = alpha * src + (1 - alpha) * rma[1]
    Initial value is SMA.
    """
    alpha = 1.0 / length
    return s.ewm(alpha=alpha, adjust=False, min_periods=length).mean()

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
    Calculates RSI exactly like TradingView using their RMA logic.
    """
    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    
    rma_up = calculate_rma(up, length)
    rma_down = calculate_rma(down, length)
    
    rsi = pd.Series(100.0, index=df.index)
    # Avoid division by zero
    rs = rma_up / rma_down.replace(0, np.nan)
    rsi_vals = 100.0 - (100.0 / (1.0 + rs))
    
    # Handle cases where down_fast == 0 (RSI = 100) and up_fast == 0 (RSI = 0)
    rsi = rsi_vals.fillna(100.0) # If down was 0, rs is NaN, fill 100
    rsi[rma_up == 0] = 0.0 # If up was 0, RSI is 0
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
