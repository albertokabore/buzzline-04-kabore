"""
consumers/project_consumer_kabore.py

Real-time insights from Dr. Case's project producer stream.

Producer JSON schema (from project_producer_case.py):
{
    "message": "...",
    "author": "...",
    "timestamp": "YYYY-MM-DD HH:MM:SS",
    "category": "humor|tech|food|travel|entertainment|gaming|other",
    "sentiment": 0.00..1.00,
    "keyword_mentioned": "...",
    "message_length": int
}

This consumer shows two live views:
  (1) Sentiment vs Time + rolling average
  (2) Top categories over a recent sliding window

Ingest mode (set in .env):
  PROJECT_INGEST_MODE=file  -> tails data/project_live.json
  PROJECT_INGEST_MODE=kafka -> consumes PROJECT_TOPIC with group PROJECT_CONSUMER_GROUP_ID

Run (from repo root):
  - Activate venv:      .\.venv\Scripts\activate
  - Consumer:           py -m consumers.project_consumer_kabore
  - Producer (separate):py -m producers.project_producer_case
"""

# ======================
# Imports & ENV
# ======================
import os, json, time, random
from pathlib import Path
from datetime import datetime
from typing import Deque, Tuple, Dict
from collections import deque, Counter

from dotenv import load_dotenv
load_dotenv()

# Logging & Kafka helper from your repo
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer

# ---- Matplotlib (Windows-friendly) ----
import matplotlib
try:
    # TkAgg is dependable on Windows; silently continue if unavailable
    if os.name == "nt":
        matplotlib.use("TkAgg")
except Exception:
    pass
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ======================
# Config (from .env)
# ======================
def _env_str(key, default): return os.getenv(key, default).strip()
def _env_int(key, default):
    try: return int(os.getenv(key, default))
    except: return default
def _env_float(key, default):
    try: return float(os.getenv(key, default))
    except: return default

MODE      = _env_str("PROJECT_INGEST_MODE", "file")  # "file" or "kafka"
TOPIC     = _env_str("PROJECT_TOPIC", "buzzline-topic")
GROUP_ID  = _env_str("PROJECT_CONSUMER_GROUP_ID", "project_group_kabore")

# Plot + analytics tuning
ROLL_N    = _env_int("PROJECT_ROLLING_WINDOW_SIZE", 30)    # rolling avg length
HIST_SIZE = _env_int("PROJECT_HISTORY_SIZE", 600)          # max sentiment points kept
BAR_WIN   = _env_int("PROJECT_BAR_WINDOW", 200)            # last N msgs for bar chart
TOPN      = _env_int("PROJECT_TOPN", 5)
FPS       = _env_float("PROJECT_PLOT_FPS", 10.0)

# Visibility controls
VERBOSE   = _env_int("PROJECT_VERBOSE", 1)                 # print each message
SHOW_LAST = _env_int("PROJECT_SHOW_LAST", 1)               # show last msg on chart

# File path
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = REPO_ROOT / "data" / "project_live.json"

# ======================
# Buffers
# ======================
TS: Deque[datetime]    = deque(maxlen=HIST_SIZE)
SENT: Deque[float]     = deque(maxlen=HIST_SIZE)
ROLLBUF: Deque[float]  = deque(maxlen=ROLL_N)
ROLLAVG: Deque[float]  = deque(maxlen=HIST_SIZE)
CATS: Deque[str]       = deque(maxlen=BAR_WIN)

_last_draw = 0.0

# ======================
# Helpers
# ======================
def parse_ts(ts_str: str) -> datetime:
    """Producer uses '%Y-%m-%d %H:%M:%S' (local, naive)."""
    try:
        return datetime.strptime(str(ts_str), "%Y-%m-%d %H:%M:%S")
    except Exception:
        # fallback: ISO or similar
        try:
            return datetime.fromisoformat(str(ts_str).replace("Z","+00:00"))
        except Exception:
            # last resort: truncate subseconds
            return datetime.strptime(str(ts_str)[:19], "%Y-%m-%d %H:%M:%S")

def normalize_value(v) -> Dict:
    """Kafka gives bytes due to value_serializer; file gives str per line."""
    if isinstance(v, dict):  return v
    if isinstance(v, bytes): v = v.decode("utf-8")
    if isinstance(v, str):   return json.loads(v)
    return json.loads(str(v))

def should_draw() -> bool:
    global _last_draw
    min_interval = 1.0 / max(FPS, 1e-6)
    now = time.perf_counter()
    if now - _last_draw >= min_interval:
        _last_draw = now
        return True
    return False

def topn_counts() -> Tuple[list, list]:
    c = Counter(CATS).most_common(TOPN)
    if not c: return ["(none)"], [0]
    labs = [k for k,_ in c]; vals = [v for _,v in c]
    return labs, vals

# ======================
# Figure
# ======================
plt.ion()
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), gridspec_kw={"height_ratios": [2, 1]})
try:
    fig.canvas.manager.set_window_title("P4 – Streaming Insights (Albert Kabore)")
except Exception:
    pass

# Time series
line_sent, = ax1.plot([], [], label="Sentiment")
line_avg,  = ax1.plot([], [], label=f"Rolling avg ({ROLL_N})", linewidth=2)
ax1.set_title("Live Sentiment Trend — by Albert Kabore")
ax1.set_xlabel("Time"); ax1.set_ylabel("Sentiment (0..1)")
ax1.set_ylim(-0.05, 1.05); ax1.grid(True, alpha=.25); ax1.legend(loc="upper left")
locator = mdates.AutoDateLocator(); formatter = mdates.ConciseDateFormatter(locator)
ax1.xaxis.set_major_locator(locator); ax1.xaxis.set_major_formatter(formatter)

# Bar chart
ax2.set_title(f"Top {TOPN} Categories (last {BAR_WIN} messages)")
ax2.set_xlabel("Category"); ax2.set_ylabel("Count"); ax2.grid(True, axis="y", alpha=.25)

# Status/overlay
wait_text = fig.text(.5, .98, "Waiting for data from producer…", ha="center", va="top", fontsize=10, alpha=.7)
last_text = ax1.text(
    0.01, 0.02, "", transform=ax1.transAxes, fontsize=9, alpha=0.9,
    bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="0.8", alpha=0.85)
)

plt.tight_layout(rect=[0,0,1,.96])
plt.show(block=False)

def redraw():
    if not should_draw():
        return
    if TS:
        line_sent.set_data(list(TS), list(SENT))
        line_avg.set_data(list(TS), list(ROLLAVG))
        ax1.relim(); ax1.autoscale_view(scalex=True, scaley=False)

    labs, vals = topn_counts()
    ax2.cla()
    ax2.set_title(f"Top {TOPN} Categories (last {BAR_WIN} messages)")
    ax2.set_xlabel("Category"); ax2.set_ylabel("Count"); ax2.grid(True, axis="y", alpha=.25)
    ax2.bar(labs, vals)

    fig.canvas.draw()
    fig.canvas.flush_events()
    plt.pause(0.05)

def process_one(obj: Dict, is_real: bool):
    """Update buffers, print/log, and redraw."""
    try:
        ts  = obj.get("timestamp")
        cat = obj.get("category", "other")
        s   = obj.get("sentiment")
        if ts is None or s is None:
            logger.debug(f"Skipping incomplete message: {obj}")
            return

        t = parse_ts(ts)
        v = float(s)

        TS.append(t)
        SENT.append(v)
        ROLLBUF.append(v)
        ROLLAVG.append(sum(ROLLBUF) / len(ROLLBUF))
        CATS.append(str(cat))

        tag = "REAL" if is_real else "SYNTH"
        if VERBOSE:
            logger.info(f"{tag} {t.strftime('%Y-%m-%d %H:%M:%S')} | {cat:<14} | sentiment={v:.2f}")
        if SHOW_LAST:
            last_text.set_text(f"{tag} · {t.strftime('%H:%M:%S')} · {cat} · s={v:.2f}")

        try:
            if wait_text.get_text():
                wait_text.set_text("")
        except Exception:
            pass

        redraw()
    except Exception as e:
        logger.error(f"Process error: {e}")

# ======================
# Loops
# ======================
def kafka_loop():
    logger.info(f"[KAFKA] topic='{TOPIC}' group='{GROUP_ID}'")
    try:
        consumer = create_kafka_consumer(TOPIC, GROUP_ID)
    except Exception as e:
        logger.error(f"Kafka consumer init failed: {e}")
        return

    try:
        while True:
            batch = consumer.poll(timeout_ms=500)
            got = False
            for _tp, msgs in batch.items():
                for m in msgs:
                    got = True
                    process_one(normalize_value(m.value), is_real=True)
            if not got:
                redraw()
    except KeyboardInterrupt:
        logger.warning("Kafka consumer stopped.")
    finally:
        try: consumer.close()
        except: pass

def file_loop():
    logger.info(f"[FILE] tailing: {DATA_FILE}")
    try:
        with DATA_FILE.open("r", encoding="utf-8") as f:
            f.seek(0, os.SEEK_END)  # tail
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.05)
                    redraw()
                    continue
                try:
                    process_one(json.loads(line), is_real=True)
                except Exception as e:
                    logger.error(f"Bad JSON line skipped: {e}")
    except FileNotFoundError:
        logger.error(f"{DATA_FILE} not found. Start the producer or check path.")
    except KeyboardInterrupt:
        logger.warning("File loop stopped.")

def main():
    logger.info(f"START consumer | mode={MODE} | backend={matplotlib.get_backend()}")
    if MODE.lower() == "kafka":
        kafka_loop()
    else:
        file_loop()

if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
