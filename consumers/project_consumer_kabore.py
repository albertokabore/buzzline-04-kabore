"""
project_consumer_kabore.py

Real-time consumer for P4.
- Primary ingest: Kafka topic from .env (PROJECT_TOPIC), default "buzzline-topic" (matches project_producer_case.py).
- Optional ingest: dynamic file tail from data/project_live.json when PROJECT_INGEST_MODE=file.

Insight: Rolling average of "sentiment" over time, plotted live with Matplotlib.
Title includes student name per assignment: "Live Rolling Sentiment by Albert Kabore".

Run from repo root to ensure 'utils' package imports correctly:
  Consumer (start first):  py -m consumers.project_consumer_kabore
  Producer:                py -m producers.project_producer_case
"""

# ------------------------- Imports -------------------------
import os
import json
import time
from collections import deque
from datetime import datetime, timezone
from typing import List, Union
from pathlib import Path

from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Local helpers from your repo
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# --------------------- Environment setup -------------------
load_dotenv()

def get_ingest_mode() -> str:
    # "kafka" (default) or "file"
    return os.getenv("PROJECT_INGEST_MODE", "kafka").strip().lower()

def get_topic() -> str:
    # Match your producer's default exactly: "buzzline-topic"
    # Falls back to BUZZ_TOPIC if provided in other modules.
    return os.getenv("PROJECT_TOPIC", os.getenv("BUZZ_TOPIC", "buzzline-topic"))

def get_group_id() -> str:
    return os.getenv("PROJECT_CONSUMER_GROUP_ID", "project_group_kabore")

def get_window() -> int:
    try:
        return int(os.getenv("PROJECT_ROLLING_WINDOW_SIZE", "30"))
    except ValueError:
        return 30

def get_data_file() -> Path:
    # Same file your project_producer_case.py writes to
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "data" / "project_live.json"

# --------------------- Parsing / normalization --------------
def parse_ts(ts: Union[str, datetime]) -> datetime:
    """
    Accepts:
      - "YYYY-mm-dd HH:MM:SS" (format used by project_producer_case.py)
      - ISO strings with 'T' and/or trailing 'Z'
      - datetime objects
    Returns timezone-aware datetime for stable plotting.
    """
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)

    s = str(ts)
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        if " " in s and "T" not in s:
            # Producer uses this format: "2025-01-29 14:35:20"
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        # ISO forms, e.g., 2025-01-29T14:35:20 or with offset
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        # Final fallback: strip microseconds if present and assume ISO T
        dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)

def normalize_value(val: Union[bytes, str, dict]) -> dict:
    """Kafka record values can arrive as dict/bytes/str; return a Python dict."""
    if isinstance(val, dict):
        return val
    if isinstance(val, bytes):
        val = val.decode("utf-8")
    if isinstance(val, str):
        return json.loads(val)
    return json.loads(str(val))

# --------------------- Stream state -------------------------
times: List[datetime] = []
sentiments: List[float] = []
rolling_avg: List[float] = []
win = deque(maxlen=get_window())

# --------------------- Live figure setup --------------------
fig, ax = plt.subplots()
plt.ion()
plt.show(block=False)  # open window immediately

line_sent = None
line_avg = None
initialized = False

def init_chart():
    """Initialize empty lines and axes formatting once."""
    global line_sent, line_avg, initialized
    if initialized:
        return

    (line_sent,) = ax.plot([], [], label="Sentiment")
    (line_avg,)  = ax.plot([], [], label=f"Rolling avg ({win.maxlen})")

    ax.set_title("Live Rolling Sentiment by Albert Kabore")
    ax.set_xlabel("Time")
    ax.set_ylabel("Sentiment (0..1)")

    # nice datetime x-axis
    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)

    ax.grid(True, alpha=0.25)
    ax.legend()
    plt.tight_layout()
    initialized = True

def update_chart():
    """Refresh line data and autoscale."""
    if not initialized:
        init_chart()

    line_sent.set_data(times, sentiments)
    line_avg.set_data(times, rolling_avg)

    if sentiments:
        ax.relim()
        ax.autoscale_view(scalex=True, scaley=True)

    plt.draw()
    plt.pause(0.01)

# --------------------- Core processing ----------------------
def process_one(obj: dict):
    """
    Extract timestamp & sentiment from one JSON message, update state & chart.
    Expected keys from producer: "timestamp" (str), "sentiment" (float 0..1)
    """
    try:
        ts = obj.get("timestamp")
        s  = obj.get("sentiment")

        if ts is None or s is None:
            logger.debug(f"Skipping message missing timestamp/sentiment: {obj}")
            return

        t = parse_ts(ts)
        v = float(s)

        times.append(t)
        sentiments.append(v)
        win.append(v)
        rolling_avg.append(sum(win) / len(win))

        update_chart()
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# --------------------- Ingestion loops ----------------------
def kafka_loop():
    topic = get_topic()
    group = get_group_id()
    logger.info(f"[KAFKA] Topic='{topic}', group='{group}', window={win.maxlen}")

    init_chart()
    update_chart()

    consumer = create_kafka_consumer(topic, group)
    try:
        for record in consumer:
            obj = normalize_value(record.value)
            process_one(obj)
    except KeyboardInterrupt:
        logger.warning("Kafka consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

def file_loop():
    path = get_data_file()
    logger.info(f"[FILE] Tailing '{path}', window={win.maxlen}")

    init_chart()
    update_chart()

    try:
        with path.open("r", encoding="utf-8") as f:
            f.seek(0, os.SEEK_END)  # jump to end; show only new messages
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.25)
                    continue
                try:
                    obj = json.loads(line)
                    process_one(obj)
                except Exception as e:
                    logger.error(f"Bad line skipped: {e}")
    except FileNotFoundError:
        logger.error(f"File not found: {path}. Start the producer first?")
    except KeyboardInterrupt:
        logger.warning("File tail interrupted by user.")

# --------------------- Entrypoint ---------------------------
def main():
    logger.info("START project_consumer_kabore")

    # clean state and reapply window from env (in case it changed)
    global win
    times.clear(); sentiments.clear(); rolling_avg.clear()
    win = deque(maxlen=get_window())

    mode = get_ingest_mode()
    logger.info(f"Ingest mode: {mode!r}")

    if mode == "file":
        file_loop()
    else:
        kafka_loop()

if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
