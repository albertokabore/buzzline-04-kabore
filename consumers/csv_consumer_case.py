"""
project_consumer_kabore.py

Live rolling sentiment visualization for P4.
- Ingest: Kafka by default (PROJECT_TOPIC), optional file tail (PROJECT_INGEST_MODE=file).
- Plot: sentiment and rolling average over time.
- Title includes student name: Albert Kabore.

Run from repo root:
  py -m consumers.project_consumer_kabore      # start consumer FIRST
  py -m producers.project_producer_case        # then producer
"""

# ----------- stdlib -----------
import os
import json
import time
from collections import deque
from datetime import datetime, timezone
from typing import Union
from pathlib import Path

# ----------- env --------------
from dotenv import load_dotenv
load_dotenv()

# ----------- matplotlib (Windows-stable backend BEFORE pyplot) -----------
import sys
import platform
import matplotlib
if os.name == "nt":
    # Prefer TkAgg on Windows for smoother interactive redraws
    matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ----------- local utils ------
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger


# ================== ENV GETTERS ==================
def get_ingest_mode() -> str:
    return os.getenv("PROJECT_INGEST_MODE", "kafka").strip().lower()

def get_topic() -> str:
    # Match producer default
    return os.getenv("PROJECT_TOPIC", os.getenv("BUZZ_TOPIC", "buzzline-topic"))

def get_group_id() -> str:
    return os.getenv("PROJECT_CONSUMER_GROUP_ID", "project_group_kabore")

def get_roll_window() -> int:
    try:
        return int(os.getenv("PROJECT_ROLLING_WINDOW_SIZE", "30"))
    except ValueError:
        return 30

def get_history_size() -> int:
    # how many points to keep in plot history
    try:
        return int(os.getenv("PROJECT_HISTORY_SIZE", "600"))
    except ValueError:
        return 600

def get_plot_fps() -> float:
    # max redraws per second
    try:
        return float(os.getenv("PROJECT_PLOT_FPS", "10"))
    except ValueError:
        return 10.0

def get_data_file() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "data" / "project_live.json"


# ================== PARSERS ==================
def parse_ts(ts: Union[str, datetime]) -> datetime:
    """Parse timestamp formats from producer; return tz-aware datetime."""
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    s = str(ts)
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        if " " in s and "T" not in s:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)

def normalize_value(val) -> dict:
    if isinstance(val, dict):
        return val
    if isinstance(val, bytes):
        val = val.decode("utf-8")
    if isinstance(val, str):
        return json.loads(val)
    return json.loads(str(val))


# ================== STREAM STATE ==================
HIST = deque(maxlen=get_history_size())    # (datetime, sentiment)
ROLL = deque(maxlen=get_roll_window())     # last N sentiments
ROLL_AVG = deque(maxlen=get_history_size())
PLOT_FPS = get_plot_fps()
_last_draw = 0.0


# ================== FIGURE SETUP ==================
fig, ax = plt.subplots()
plt.ion()
plt.show(block=False)

line_sent, = ax.plot([], [], label="Sentiment")
line_avg,  = ax.plot([], [], label=f"Rolling avg ({ROLL.maxlen})")

# nicer datetime axis
locator = mdates.AutoDateLocator()
formatter = mdates.ConciseDateFormatter(locator)
ax.xaxis.set_major_locator(locator)
ax.xaxis.set_major_formatter(formatter)

# title includes your name
ax.set_title("Live Rolling Sentiment by Albert Kabore")
ax.set_xlabel("Time")
ax.set_ylabel("Sentiment (0..1)")
ax.grid(True, alpha=0.25)
ax.legend()
try:
    # nicer window title if supported
    fig.canvas.manager.set_window_title("P4 – Rolling Sentiment (Albert Kabore)")
except Exception:
    pass
plt.tight_layout()


def _should_redraw() -> bool:
    global _last_draw
    now = time.perf_counter()
    min_interval = 1.0 / max(PLOT_FPS, 1e-6)
    if now - _last_draw >= min_interval:
        _last_draw = now
        return True
    return False


def update_chart():
    """Efficient redraw without clearing axes; rate-limited by FPS."""
    if not _should_redraw():
        return
    if not HIST:
        return

    xs = [t for (t, _) in HIST]
    ys = [v for (_, v) in HIST]
    ra = list(ROLL_AVG)

    line_sent.set_data(xs, ys)
    line_avg.set_data(xs, ra)

    ax.relim()
    ax.autoscale_view(scalex=True, scaley=True)

    # ensure GUI stays responsive
    fig.canvas.draw_idle()
    fig.canvas.flush_events()
    plt.pause(0.001)


def process_one(obj: dict):
    """Extract timestamp and sentiment, update state & plot."""
    try:
        ts = obj.get("timestamp")
        s = obj.get("sentiment")
        if ts is None or s is None:
            logger.debug(f"Skipping message missing timestamp/sentiment: {obj}")
            return

        t = parse_ts(ts)
        v = float(s)

        HIST.append((t, v))
        ROLL.append(v)
        ROLL_AVG.append(sum(ROLL) / len(ROLL))

        update_chart()
    except Exception as e:
        logger.error(f"Error processing message: {e}")


# ================== LOOPS ==================
def kafka_loop():
    topic = get_topic()
    group = get_group_id()
    logger.info(f"[KAFKA] Topic='{topic}' | Group='{group}' | RollWindow={ROLL.maxlen} | FPS={PLOT_FPS}")

    # draw an empty frame so the window appears immediately
    update_chart()

    try:
        consumer = create_kafka_consumer(topic, group)
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        logger.error("Is Kafka running? Is KAFKA_SERVER correct in .env?")
        return

    try:
        for record in consumer:
            obj = normalize_value(record.value)
            process_one(obj)
    except KeyboardInterrupt:
        logger.warning("Kafka consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Kafka loop error: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("Kafka consumer closed.")


def file_loop():
    path = get_data_file()
    logger.info(f"[FILE] Tailing '{path}' | RollWindow={ROLL.maxlen} | FPS={PLOT_FPS}")

    update_chart()

    try:
        with path.open("r", encoding="utf-8") as f:
            f.seek(0, os.SEEK_END)
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.05)
                    update_chart()
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


# ================== MAIN ==================
def main():
    # reset deques with current env sizes each run
    global HIST, ROLL, ROLL_AVG, PLOT_FPS, _last_draw
    HIST = deque(maxlen=get_history_size())
    ROLL = deque(maxlen=get_roll_window())
    ROLL_AVG = deque(maxlen=get_history_size())
    PLOT_FPS = get_plot_fps()
    _last_draw = 0.0

    mode = get_ingest_mode()
    logger.info(f"START consumer | Mode={mode} | Backend={matplotlib.get_backend()} | Python={sys.version.split()[0]} | OS={platform.system()}")

    if mode == "file":
        file_loop()
    else:
        kafka_loop()


if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
