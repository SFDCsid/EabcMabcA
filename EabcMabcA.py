# EabcMabcA.py

import os
import time
import requests
import pandas as pd
import datetime
import logging
from fyers_apiv3 import fyersModel
from io import StringIO

# ============================ 
# Setup Logging (console + file)
# ============================ 
os.makedirs("logs", exist_ok=True)
_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = f"logs/run-{_timestamp}.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

log = logging.info  # simple info messages

# ============================
# Telegram sending helper (safe)
# ============================
def safe_send_telegram(text: str):
    try:
        bot = os.environ.get("TG_BOT_TOKEN")
        chat = os.environ.get("TG_CHAT_ID")
        if not bot or not chat:
            print(f"⚠️ Telegram not configured: {text}")
            return
        url = f"https://api.telegram.org/bot{bot}/sendMessage"
        payload = {"chat_id": chat, "text": text}
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code != 200:
            print(f"⚠️ Telegram send failed: {r.text}")
    except Exception as e:
        print(f"⚠️ Telegram exception: {e}")

# ============================
# Override warn() and error()
# ============================
def warn(msg):
    logging.warning(msg)
    safe_send_telegram(f"⚠️ WARNING:\n{msg}")

def error(msg):
    logging.error(msg)
    safe_send_telegram(f"❌ ERROR:\n{msg}")

log(f"📝 Logging to {LOG_FILE}")

# ============================ 
# Load Fyers token
# ============================ 
access_token = os.environ.get("FYERS_TOKEN")
if not access_token:
    error("❌ FYERS_TOKEN not set in secrets!")
    raise ValueError("❌ FYERS_TOKEN not set in secrets!")

client_id = access_token.split(":")[0]
fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False)
log("🔑 Fyers token loaded")

# ============================ 
# Load Telegram credentials
# ============================ 
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
CHAT_ID   = os.environ.get("TG_CHAT_ID")
if not BOT_TOKEN or not CHAT_ID:
    error("❌ Telegram credentials not set in secrets!")
    raise ValueError("❌ Telegram credentials not set in secrets!")

# ============================ 
# Load configs from CSV
# ============================ 
configs_str = os.environ.get("TRADE_CONFIGS")
if not configs_str:
    error("❌ TRADE_CONFIGS variable not set in GitHub Actions!")
    raise ValueError("❌ TRADE_CONFIGS variable not set in GitHub Actions!")

configs_df = pd.read_csv(StringIO(configs_str))
configs = list(configs_df.itertuples(index=False, name=None))

log("✅ Loaded strategy configs from GitHub Actions variable:")
log(str(configs_df))

# ============================ 
# Test Telegram (uncomment this for just telegram message testing)
# ============================ 
# send_msg_test = "🧪 Test: Telegram alerts are configured and working!"
# log(send_msg_test)
# safe_send_telegram(send_msg_test)

# ============================ 
# Data functions
# ============================ 
def fetch_candles(symbol, resolution, count=2000):
    end_time = int(time.time())
    start_time = end_time - (count * int(resolution) * 60)
    data = {
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "0",
        "range_from": start_time,
        "range_to": end_time,
        "cont_flag": "1"
    }
    resp = fyers.history(data=data)
    if resp.get("s") != "ok":
        error(f"❌ Error fetching {symbol} {resolution}m: {resp}")
        return None
    df = pd.DataFrame(resp["candles"], columns=["Timestamp","Open","High","Low","Close","Volume"])
    df["Timestamp"] = (
        pd.to_datetime(df["Timestamp"], unit="s", utc=True)
        .dt.tz_convert("Asia/Kolkata")
        .dt.tz_localize(None)
    )
    return df.sort_values("Timestamp").reset_index(drop=True)

def add_ema(df, periods):
    for p in periods:
        df[f"EMA_{p}"] = df["Close"].ewm(span=p, adjust=False).mean()
    return df

def detect_and_alert_crossovers(df, periods, symbol, tf):
    if df is None or len(df) < 2:
        warn("⚠️ Not enough data to check crossovers")
        return
    last = df.iloc[-1]
    prev = df.iloc[-2]
    for p in periods:
        crossed_up   = prev["Close"] < prev[f"EMA_{p}"] and last["Close"] > last[f"EMA_{p}"]
        crossed_down = prev["Close"] > prev[f"EMA_{p}"] and last["Close"] < last[f"EMA_{p}"]
        if crossed_up:
            msg = (
                f"📈 {symbol} | {tf}m\n"
                f"🕒 {last['Timestamp']}\n"
                f"Cross ABOVE EMA{p}\n"
                f"Close: {last['Close']:.2f} | EMA: {last[f'EMA_{p}']:.2f}"
            )
            log(msg)
            safe_send_telegram(msg)
        elif crossed_down:
            msg = (
                f"📉 {symbol} | {tf}m\n"
                f"🕒 {last['Timestamp']}\n"
                f"Cross BELOW EMA{p}\n"
                f"Close: {last['Close']:.2f} | EMA: {last[f'EMA_{p}']:.2f}"
            )
            log(msg)
            safe_send_telegram(msg)
        else:
            log(
                f"No crossover | {symbol} {tf}m | {last['Timestamp']} | "
                f"Close={last['Close']:.2f} | EMA{p}={last[f'EMA_{p}']:.2f}"
            )

# ============================ 
# Main Loop
# ============================ 
try:
    for symbol, tf, ema_p, count in configs:
        log(f"\n📊 {symbol} | {tf} min timeframe | EMA{ema_p} | count={count}")
        df = fetch_candles(symbol, str(tf), int(count))
        if df is not None:
            df = add_ema(df, [int(ema_p)])
            log(str(df.tail(3)[["Timestamp","Close", f"EMA_{ema_p}"]]))
            detect_and_alert_crossovers(df, [int(ema_p)], symbol, str(tf))
    log("✅ Run completed successfully")
except Exception as e:
    error(f"💥 Unhandled exception: {e}")
    raise
