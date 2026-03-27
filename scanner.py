import pandas as pd
import indicators
import data_loader
import concurrent.futures
import pytz
from datetime import datetime

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_rsi_div(symbol, interval, settings):
    """
    Scans a single symbol fetching its own data.
    """
    df = data_loader.fetch_data(symbol, interval=interval)
    return scan_symbol_rsi_div_prefetched(symbol, df, interval, settings)

def scan_symbol_rsi_div_prefetched(symbol, df, interval, settings):
    """
    Scans a single symbol for RSI Divergence signals using a pre-fetched DataFrame.
    """
    try:
        if df is None or df.empty or len(df) < 50:
            return None
            
        rsi_fast = settings.get("rsi_fast", 5)
        rsi_slow = settings.get("rsi_slow", 14)
        
        # Calculate RSI Divergence
        res_df = indicators.calculate_rsi_divergence(
            df, 
            rsi_fast_len=rsi_fast,
            rsi_slow_len=rsi_slow
        )
        
        if res_df.empty:
            return None
            
        signals = res_df[res_df['Signal'] != 0].copy()
        
        if signals.empty:
            return None
            
        # Date Range Filtering
        start_date = settings.get("start_date")
        end_date = settings.get("end_date")
        
        if start_date and end_date:
            try:
                tz = pytz.timezone('Asia/Kolkata')
                s_dt = tz.localize(datetime.combine(start_date, datetime.min.time()))
                e_dt = tz.localize(datetime.combine(end_date, datetime.max.time()))
                signals = signals[(signals.index >= s_dt) & (signals.index <= e_dt)]
            except Exception as e:
                pass
        
        if signals.empty:
            return []
            
        current_bar = res_df.iloc[-1]
        current_ltp = current_bar['close']
        
        symbol_results = []
        
        for idx, signal_row in signals.iterrows():
            signal_price = round(signal_row['close'], 2)
            signal_type_str = "Buy (Long)" if signal_row['Signal'] == 1 else "Sell (Short)"
            
            atr_val = signal_row.get('ATR', current_bar.get('ATR', 0))
            bb_lower = signal_row.get('BBL', 0)
            bb_upper = signal_row.get('BBU', 0)
            signal_trend = signal_row.get('Trend', current_bar.get('Trend', 'N/A'))
            signal_ema9 = signal_row.get('EMA9', current_bar.get('EMA9', 0))
            signal_ema21 = signal_row.get('EMA21', current_bar.get('EMA21', 0))
            signal_volume = signal_row.get('volume', current_bar.get('volume', 0))
            
            # Additional Indicators for display
            fast_rsi = signal_row.get('Fast_RSI', 0)
            slow_rsi = signal_row.get('Slow_RSI', 0)
            rsi_div = signal_row.get('RSI_Divergence', 0)
            
            # Simple ATR Stop Loss / Take profit (1:2 R:R)
            if signal_row['Signal'] == 1: # Buy
                atr_sl = signal_price - (atr_val * 1.5)
                atr_tp = signal_price + (atr_val * 3)
                ema_sl_str = f"₹{round(signal_ema21, 2)}" if signal_ema21 < signal_price else f"₹{round(signal_ema21, 2)} ⏳"
            else: # Sell
                atr_sl = signal_price + (atr_val * 1.5)
                atr_tp = signal_price - (atr_val * 3)
                ema_sl_str = f"₹{round(signal_ema21, 2)}" if signal_ema21 > signal_price else f"₹{round(signal_ema21, 2)} ⏳"

            symbol_results.append({
                "Stock": symbol,
                "LTP": round(current_ltp, 2),
                "Signal Time": idx,
                "Signal Type": signal_type_str,
                "Signal Price": signal_price,
                "RSI Fast/Slow": f"{round(fast_rsi, 1)} / {round(slow_rsi, 1)}",
                "RSI Div": round(rsi_div, 2),
                "ATR (SL/TP)": f"₹{round(atr_sl, 2)} / ₹{round(atr_tp, 2)}",
                "EMA SL": ema_sl_str,
                "ATR": round(atr_val, 2),
                "BB Lower": round(bb_lower, 2),
                "BB Upper": round(bb_upper, 2),
                "Trend": signal_trend,
                "EMA9": round(signal_ema9, 2),
                "EMA21": round(signal_ema21, 2),
                "Volume": int(signal_volume)
            })
            
        return symbol_results

    except Exception as e:
        return None

def scan_market(symbols, interval='1d', settings=None, progress_callback=None):
    """
    Parallel bulk scan of market symbols using pre-fetched block data.
    """
    results = []
    if settings is None:
        settings = {}
        
    bulk_data_dict = data_loader.fetch_bulk_data(symbols, interval=interval, progress_callback=progress_callback)
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(scan_symbol_rsi_div_prefetched, sym, bulk_data_dict.get(sym), interval, settings): sym 
            for sym in symbols
        }
        
        for future in concurrent.futures.as_completed(futures):
            res_list = future.result()
            if res_list:
                results.extend(res_list)
                
    return pd.DataFrame(results)
