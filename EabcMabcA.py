import os, time, requests, pandas as pd
from fyers_apiv3 import fyersModel

# ============================ 
# Load Fyers token from environment
# ============================
access_token = os.environ.get("FYERS_TOKEN")
if not access_token:
    raise ValueError("❌ FYERS_TOKEN not set in secrets!")
client_id = access_token.split(":")[0]
fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False)
print("🔑 Fyers token loaded")

# ============================ 
# Load Telegram creds from environment
# ============================
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
CHAT_ID   = os.environ.get("TG_CHAT_ID")
if not BOT_TOKEN or not CHAT_ID:
    raise ValueError("❌ Telegram credentials not set in secrets!")

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try:
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code != 200:
            print("⚠️ Telegram send failed:", r.text)
    except Exception as e:
        print("⚠️ Telegram Error:", e)

# ============================ 
# Load configs from CSV (kept in repo)
# ============================
CONFIG_FILE = "configs.csv"   # place in same repo folder

if not os.path.exists(CONFIG_FILE):
    raise FileNotFoundError(f"❌ Config file not found: {CONFIG_FILE}")

configs_df = pd.read_csv(CONFIG_FILE)
configs = list(configs_df.itertuples(index=False, name=None))

print("✅ Loaded strategy configs from CSV:")
print(configs_df)

# Optional test message
send_telegram_message("🧪 Test: Telegram alerts are configured and working!")

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
        print(f"❌ Error fetching {symbol} {resolution}m:", resp)
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
        print("⚠️ Not enough data to check crossovers")
        return
    last = df.iloc[-1]
    prev = df.iloc[-2]
    for p in periods:
        crossed_up   = prev["Close"] < prev[f"EMA_{p}"] and last["Close"] > last[f"EMA_{p}"]
        crossed_down = prev["Close"] > prev[f"EMA_{p}"] and last["Close"] < last[f"EMA_{p}"]
        if crossed_up:
            msg = f"📈 {symbol} | {tf}m\n🕒 {last['Timestamp']}\nCross ABOVE EMA{p}\nClose: {last['Close']:.2f} | EMA: {last[f'EMA_{p}']:.2f}"
            print(msg)
            send_telegram_message(msg)
        elif crossed_down:
            msg = f"📉 {symbol} | {tf}m\n🕒 {last['Timestamp']}\nCross BELOW EMA{p}\nClose: {last['Close']:.2f} | EMA: {last[f'EMA_{p}']:.2f}"
            print(msg)
            send_telegram_message(msg)
        else:
            print(f"No crossover | {symbol} {tf}m | {last['Timestamp']} | Close={last['Close']:.2f} | EMA{p}={last[f'EMA_{p}']:.2f}")

# ============================ 
# Main Loop (runs for each config)
# ============================
for symbol, tf, ema_p, count in configs:
    print(f"\n📊 {symbol} | {tf} min timeframe | EMA{ema_p} | count={count}")
    df = fetch_candles(symbol, str(tf), int(count))
    if df is not None:
        df = add_ema(df, [int(ema_p)])
        print(df.tail(3)[["Timestamp","Close", f"EMA_{ema_p}"]])
        detect_and_alert_crossovers(df, [int(ema_p)], symbol, str(tf))
