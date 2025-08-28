# ============================
# SMA Crossover Alert with Gap Handling and Separate Test Telegram Message
# ============================

import os
import time
import requests
import pandas as pd
import datetime
import logging
from fyers_apiv3 import fyersModel
from io import StringIO

# ============================ 
# Setup Logging
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
log = logging.info
log(f"📝 Logging to {LOG_FILE}")

# ============================
# Load Telegram credentials
# ============================
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
CHAT_ID = os.environ.get("TG_CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    log("⚠️ Telegram credentials not set. Telegram alerts will not work.")

TELEGRAM_LIMIT = 4000  # Telegram max message length

# ============================
# Independent Test Telegram Message
# ============================
#SEND_TEST_TELEGRAM = True  # <-- direct toggle here
SEND_TEST_TELEGRAM = os.environ.get("SEND_TEST_TELEGRAM")

def send_test_telegram():
    """Send a standalone test message, independent of other alerts."""
    if SEND_TEST_TELEGRAM and BOT_TOKEN and CHAT_ID:
        msg = "🧪 Test: Telegram alerts are configured and working!"
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": msg}
        try:
            r = requests.post(url, data=payload, timeout=10)
            if r.status_code == 200:
                log("✅ Test Telegram message sent successfully.")
            else:
                log(f"⚠️ Test Telegram failed: {r.text}")
        except Exception as e:
            log(f"⚠️ Test Telegram exception: {e}")

# Send test message immediately
send_test_telegram()

# ============================
# In-memory storage for alerts
# ============================
all_alerts = []
alert_cache = set()  # For duplicate prevention per run

# ============================
# Telegram helpers (for SMA alerts)
# ============================
def safe_send_telegram_bulk(messages):
    if not BOT_TOKEN or not CHAT_ID:
        log("⚠️ Telegram not configured")
        return
    combined = "\n\n".join(messages)
    while combined:
        chunk = combined[:TELEGRAM_LIMIT]
        combined = combined[TELEGRAM_LIMIT:]
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": chunk}
        try:
            r = requests.post(url, data=payload, timeout=10)
            if r.status_code != 200:
                log(f"⚠️ Telegram send failed: {r.text}")
        except Exception as e:
            log(f"⚠️ Telegram exception: {e}")

# ============================
# Logging overrides to queue alerts
# ============================
def warn(msg):
    logging.warning(msg)
    all_alerts.append(f"⚠️ WARNING: {msg}")

def error(msg):
    logging.error(msg)
    all_alerts.append(f"❌ ERROR: {msg}")

# ============================
# Fyers setup
# ============================
access_token = os.environ.get("FYERS_TOKEN")
if not access_token:
    error("❌ FYERS_TOKEN not set in secrets!")
    raise ValueError("❌ FYERS_TOKEN not set in secrets!")

client_id = access_token.split(":")[0]
fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False)
log("🔑 Fyers token loaded")

# ============================
# Load configs from CSV
# ============================
configs_str = os.environ.get("TRADE_CONFIGS")
if not configs_str:
    error("❌ TRADE_CONFIGS variable not set in GitHub Actions!")
    raise ValueError("❌ TRADE_CONFIGS variable not set in GitHub Actions!")

try:
    configs_df = pd.read_csv(StringIO(configs_str))
except pd.errors.EmptyDataError:
    error("❌ TRADE_CONFIGS provided but CSV is empty!")
    raise ValueError("❌ TRADE_CONFIGS provided but CSV is empty!")

configs = list(configs_df.itertuples(index=False, name=None))
log("✅ Loaded strategy configs:")
log(str(configs_df))

# ============================
# Fetch historical candles
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

# ============================
# Add SMA
# ============================
def add_sma(df, periods):
    for p in periods:
        df[f"SMA_{p}"] = df["Close"].rolling(window=p).mean()
    return df

# ============================
# Detect SMA crossovers with gap detection
# ============================
def detect_sma_cross(df, periods, symbol, tf):
    if df is None or len(df) < 2:
        warn("⚠️ Not enough data to check crossovers")
        return

    last = df.iloc[-1]
    prev = df.iloc[-2]

    for p in periods:
        sma = last[f"SMA_{p}"]
        key = f"{symbol}_{tf}_SMA{p}"  # Duplicate prevention

        if key in alert_cache:
            continue

        crossed_prev = (prev["Open"] < sma < prev["Close"]) or (prev["Open"] > sma > prev["Close"])
        crossed_last = (last["Open"] < sma < last["Close"]) or (last["Open"] > sma > last["Close"])
        crossed_gap  = (prev["Close"] < sma < last["Open"]) or (prev["Close"] > sma > last["Open"])

        if crossed_prev or crossed_last or crossed_gap:
            trend = "Bullish" if last["Close"] > sma else "Bearish"
            gap_info = " (Gap detected)" if crossed_gap else ""

            ts_24 = last['Timestamp'].strftime("%Y-%m-%d %H:%M:%S")
            ts_12 = last['Timestamp'].strftime("%Y-%m-%d %I:%M:%S %p")

            msg = (
                f"{'📈' if trend=='Bullish' else '📉'} {symbol} | {tf}m\n"
                f"🕒 {ts_12} / {ts_24}\n"
                f"Cross {'ABOVE' if trend=='Bullish' else 'BELOW'} SMA{p} {trend}{gap_info}\n"
                f"Close: {last['Close']:.2f} | SMA: {sma:.2f}"
            )
            all_alerts.append(msg)
            alert_cache.add(key)

# ============================
# Main Loop
# ============================
try:
    for symbol, tf, sma_p, count in configs:
        log(f"\n📊 {symbol} | {tf} min timeframe | SMA{sma_p} | count={count}")
        df = fetch_candles(symbol, str(tf), int(count))
        if df is not None:
            periods = [int(sma_p)] if isinstance(sma_p, int) else list(map(int, str(sma_p).split(";")))
            df = add_sma(df, periods)
            log(str(df.tail(3)[["Timestamp","Close"] + [f"SMA_{p}" for p in periods]]))
            detect_sma_cross(df, periods, symbol, str(tf))

    # Send all SMA alerts in bulk if any
    if all_alerts:
        safe_send_telegram_bulk(all_alerts)
    else:
        log("✅ No alerts detected this run.")

    log("✅ Run completed successfully")

except Exception as e:
    error(f"💥 Unhandled exception: {e}")
    if all_alerts:
        safe_send_telegram_bulk(all_alerts)
    raise
