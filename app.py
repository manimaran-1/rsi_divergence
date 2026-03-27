import streamlit as st
import pandas as pd
import scanner
import data_loader
import pytz
from datetime import datetime

st.set_page_config(page_title="RSI Divergence Scanner", layout="wide")

# --- SECURITY & UI CONFIG ---
hide_st_style = '''
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
    .stDeployButton, [data-testid="stAppDeployButton"] {display: none !important;}
    .stGithubButton, [data-testid="stToolbarActionButton"] {display: none !important;}
</style>
'''
st.markdown(hide_st_style, unsafe_allow_html=True)
import hmac

# Secure password check using st.secrets
if "password_correct" not in st.session_state:
    st.session_state.password_correct = False

def check_password():
    if "password" in st.secrets:
        if st.session_state.password_correct:
            return True
        st.markdown("""
            <h1 style='text-align: center; margin-top: 50px;'>🔐 Secure Access</h1>
        """, unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with st.form("login_form"):
                password = st.text_input("Password", type="password", key="login_password")
                submit = st.form_submit_button("Login", use_container_width=True)
            if submit:
                # Convert the secret to string to match input reliably
                if password == str(st.secrets.get("password", "")):
                    st.session_state.password_correct = True
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")
        return False
    else:
        # If no password is set in secrets, allow access for local testing
        st.warning("⚠️ No password configured in secrets.toml. Proceeding without authentication.")
        return True

if not check_password():
    st.stop()

# -----------------------------

st.title("RSI Divergence Scanner 📈")
st.markdown("Scan NSE stocks for RSI Divergence crossovers based on TradingView script by Shizaru.")

# Sidebar Controls
st.sidebar.header("Configuration")

# Universe Selection
indices_dict = data_loader.get_all_indices_dict()
universe_options = list(indices_dict.keys()) + ["Custom List"]
selected_universe = st.sidebar.selectbox("Select Stock Universe", universe_options)

# Timeframe Selection
timeframe_options = ["1d", "1wk", "1mo", "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"]
selected_timeframe = st.sidebar.selectbox("Select Timeframe", timeframe_options)

# Date Range Selection
st.sidebar.subheader("Date Range")
import datetime
today = datetime.datetime.now().date()

# Dynamically adjust default search range based on timeframe
if selected_timeframe == "1m":
    default_days = 3
elif selected_timeframe in ["2m", "5m"]:
    default_days = 5
elif selected_timeframe in ["15m", "30m", "60m", "90m", "1h"]:
    default_days = 7 # 1 week
elif selected_timeframe == "1d":
    default_days = 30 # 1 month
elif selected_timeframe == "1wk":
    default_days = 180 # 6 months
elif selected_timeframe == "1mo":
    default_days = 365 # 1 year
else:
    default_days = 7

default_start = today - datetime.timedelta(days=default_days)

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(default_start, today),
    max_value=today
)

start_date = None
end_date = None
if isinstance(date_range, tuple):
    if len(date_range) == 2:
        start_date, end_date = date_range
    elif len(date_range) == 1:
        start_date = date_range[0]
        end_date = start_date
elif isinstance(date_range, datetime.date):
    start_date = date_range
    end_date = date_range

# RSI Settings (Included as per core logic requirements)
st.sidebar.subheader("Indicator Settings")
rsi_fast = st.sidebar.number_input("Fast RSI Length", min_value=1, max_value=100, value=5)
rsi_slow = st.sidebar.number_input("Slow RSI Length", min_value=1, max_value=100, value=14)

st.sidebar.markdown("---")
st.sidebar.info("**Timezone**: IST (Asia/Kolkata)")
st.sidebar.info("**Data Source**: Yahoo Finance (IST Optimized)")
st.sidebar.info("**Note**: Intraday scans show all signals from today.")

# Load Symbols based on selection
symbols = []

if selected_universe == "Custom List":
    custom_input = st.sidebar.text_area("Enter symbols (comma separated)", "RELIANCE.NS, INFY.NS")
    if custom_input:
        symbols = [s.strip() for s in custom_input.split(",")]
else:
    # It's an index selection
    with st.spinner(f"Fetching {selected_universe} symbols..."):
        if selected_universe == "Nifty 500":
            symbols = data_loader.get_nifty500_symbols()
        elif selected_universe == "Nifty 200":
            symbols = data_loader.get_nifty200_symbols()
        elif selected_universe == "Nifty 50":
            symbols = data_loader.get_nifty200_symbols()[:50]
        else:
            symbols = data_loader.get_index_constituents(selected_universe)
            
        if not symbols:
            st.warning(f"Could not fetch symbols for {selected_universe}. Using fallback Nifty 50 list.")
            symbols = data_loader.get_nifty200_symbols()[:50]

st.write(f"**Universe:** {selected_universe} ({len(symbols)} symbols)")
st.write(f"**Timeframe:** {selected_timeframe}")

# Run Scan Button
if st.button("Run Scanner"):
    if not symbols:
        st.error("No symbols selected.")
    else:
        st.write(f"Scanning {len(symbols)} stocks... This may take a while.")
        
        scan_settings = {
            "rsi_fast": rsi_fast,
            "rsi_slow": rsi_slow,
            "start_date": start_date,
            "end_date": end_date
        }
        
        with st.spinner("Processing..."):
            results_df = scanner.scan_market(symbols, interval=selected_timeframe, settings=scan_settings)
        
        if not results_df.empty:
            st.success(f"Found {len(results_df)} signal(s)!")
            
            # Sort by Signal Time descending
            results_df = results_df.sort_values(by='Signal Time', ascending=False)
            
            # Display Main Table
            st.dataframe(
                results_df,
                column_config={
                    "Stock": "Stock",
                    "Signal Time": "Time (IST)",
                    "LTP": st.column_config.NumberColumn("LTP", format="₹ %.2f"),
                    "Signal Price": st.column_config.NumberColumn("Signal Price", format="₹ %.2f"),
                    "RSI Fast/Slow": "RSI F/S",
                    "RSI Div": st.column_config.NumberColumn("RSI Div", format="%.2f"),
                    "ATR (SL/TP)": "ATR SL/TP",
                    "EMA SL": "EMA SL",
                    "Trend": "Trend",
                    "Volume": st.column_config.NumberColumn("Volume", format="%d"),
                },
                hide_index=True,
                width="stretch"
            )
            
            # Download option
            csv = results_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download CSV",
                csv,
                f"rsi_div_scan_results_{selected_universe}_{selected_timeframe}.csv",
                "text/csv",
                key='download-csv'
            )
        else:
            st.warning("No stocks matched the criteria/timeframe conditions.")

with st.expander("View Logic Details"):
    st.markdown("""
    **RSI Divergence Conditions:**
    1. **Fast RSI**: Length 5
    2. **Slow RSI**: Length 14
    3. **RSI Divergence**: Fast RSI - Slow RSI
    4. **Buy (Long) Signal**: RSI Divergence crosses *above* 0 from below.
    5. **Sell (Short) Signal**: RSI Divergence crosses *below* 0 from above.
    """)
