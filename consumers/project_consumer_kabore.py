"""
project_consumer_kabore.py

P4 – Real-time consumer with two insights:
  (1) Rolling SENTIMENT over time (line + rolling avg)
  (2) Top CATEGORY counts over a recent sliding window (bar)

Author: Albert Kabore

Ingest:
  - Kafka (default) using PROJECT_* variables in .env
  - File tail of data/project_live.json if PROJECT_INGEST_MODE=file

NEVER-EMPTY MODE:
  If no messages arrive for N seconds, optionally synthesize messages to keep
  the visualization alive (for demos/troubleshooting). Controlled by:
    PROJECT_FAKE_IF_IDLE_SECONDS (int seconds; 0 disables; default 10)

Run (from repo root):
  Consumer (start first):  py -m consumers.project_consumer_kabore
  Producer:                py -m producers.project_producer_case
"""

# ---------------- stdlib ----------------
import os
import json
import time
import random
from collections import deque, Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Union, Tuple, Dict

# ---------------- env -------------------
from dotenv import load_dotenv
load_dotenv()

# ---------------- matplotlib ------------
import matplotlib
if os.name == "nt":
    matplotlib.use("TkAgg")  # smoother on Windows
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ---------------- repo utils ------------
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger


# ============ ENV GETTERS ============
def get_ingest_mode() -> str:
    return os.getenv("PROJECT_INGEST_MODE", "kafka").strip().lower()

def get_topic() -> str:
    return os.getenv("PROJECT_TOPIC", os.getenv("BUZZ_TOPIC", "buzzline-topic"))

def get_group_id() -> str:
    return os.getenv("PROJECT_CONSUMER_GROUP_ID", "project_group_kabore")

def get_roll_window() -> int:
    try: return int(os.getenv("PROJECT_ROLLING_WINDOW_SIZE", "30"))
    except ValueError: return 30

def get_history_size() -> int:
    try: return int(os.getenv("PROJECT_HISTORY_SIZE", "600"))
    except ValueError: return 600

def get_bar_window() -> int:
    try: return int(os.getenv("PROJECT_BAR_WINDOW", "200"))
    except ValueError: return 200

def get_topn() -> int:
    try: return int(os.getenv("PROJECT_TOPN", "5"))
    except ValueError: return 5

def get_plot_fps() -> float:
    try: return float(os.getenv("PROJECT_PLOT_FPS", "10"))
    except ValueError: return 10.0

def get_fake_idle_seconds() -> int:
    # 0 disables synthetic fallback; default 10s to keep charts alive
    try: return int(os.getenv("PROJECT_FAKE_IF_IDLE_SECONDS", "10"))
    except ValueError: return 10

def get_data_file() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "data" / "project_live.json"


# ============ PARSERS ============
def parse_ts(ts: Union[str, datetime]) -> datetime:
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
    if isinstance(val, dict): return val
    if isinstance(val, bytes): val = val.decode("utf-8")
    if isinstance(val, str): return json.loads(val)
    return json.loads(str(val))


# ============ SYNTHETIC (fallback) ============
CATEGORIES = ["humor", "tech", "food", "travel", "entertainment", "gaming", "other"]

def synth_message() -> Dict[str, object]:
    """Produce a realistic message shaped like the producer."""
    sentiment = round(random.uniform(0.1, 0.95), 2)
    category = random.choice(CATEGORIES)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"Synth event in {category} with sentiment {sentiment}"
    return {
        "message": msg,
        "author": random.choice(["Alice","Bob","Charlie","Eve"]),
        "timestamp": now,
        "category": category,
        "sentiment": sentiment,
        "keyword_mentioned": category,
        "message_length": len(msg),
    }


# ============ STREAM STATE ============
ROLL_WIN = get_roll_window()
HIST_SIZE = get_history_size()
BAR_SIZE = get_bar_window()
TOPN = get_topn()
FPS = get_plot_fps()
FAKE_IDLE = get_fake_idle_seconds()

# time series buffers
TS = deque(maxlen=HIST_SIZE)          # datetimes
SENT = deque(maxlen=HIST_SIZE)        # floats
ROLL = deque(maxlen=ROLL_WIN)         # last N for rolling avg
ROLL_AVG = deque(maxlen=HIST_SIZE)    # floats

# categories sliding window
CAT_WIN = deque(maxlen=BAR_SIZE)

_last_draw = 0.0
_last_real_msg_monotonic = time.perf_counter()
_seen_real = False


# ============ FIGURE SETUP ============
fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=1, figsize=(10, 7), gridspec_kw=dict(height_ratios=[2, 1]))
plt.ion()
plt.show(block=False)

# Panel 1: sentiment over time
line_sent, = ax1.plot([], [], label="Sentiment")
line_avg,  = ax1.plot([], [], label=f"Rolling avg ({ROLL_WIN})", linewidth=2)
ax1.set_title("Live Rolling Sentiment by Albert Kabore")
ax1.set_xlabel("Time")
ax1.set_ylabel("Sentiment (0..1)")
ax1.set_ylim(-0.05, 1.05)
ax1.grid(True, alpha=0.25)
ax1.legend(loc="upper left")
locator = mdates.AutoDateLocator()
formatter = mdates.ConciseDateFormatter(locator)
ax1.xaxis.set_major_locator(locator)
ax1.xaxis.set_major_formatter(formatter)

# Panel 2: top categories
ax2.set_title(f"Top {TOPN} Categories (last {BAR_SIZE} msgs)")
ax2.set_xlabel("Category")
ax2.set_ylabel("Count")
ax2.grid(True, axis="y", alpha=0.25)

try:
    fig.canvas.manager.set_window_title("P4 – Streaming Insights (Albert Kabore)")
except Exception:
    pass

wait_text = fig.text(0.5, 0.98, "Waiting for data...", ha="center", va="top", fontsize=10, alpha=0.7)
plt.tight_layout(rect=[0, 0, 1, 0.96])  # leave room for wait_text


# ============ DRAW HELPERS ============
def _should_redraw() -> bool:
    global _last_draw
    now = time.perf_counter()
    min_interval = 1.0 / max(FPS, 1e-6)
    if now - _last_draw >= min_interval:
        _last_draw = now
        return True
    return False

def _top_n_counts() -> Tuple[list, list]:
    counts = Counter(CAT_WIN)
    most = counts.most_common(TOPN)
    labels = [k for k, _ in most] or ["(none)"]
    values = [v for _, v in most] or [0]
    return labels, values

def update_chart():
    if not _should_redraw():
        return

    # Panel 1
    if TS:
        line_sent.set_data(list(TS), list(SENT))
        line_avg.set_data(list(TS), list(ROLL_AVG))
        ax1.relim()
        ax1.autoscale_view(scalex=True, scaley=False)

    # Panel 2
    labels, values = _top_n_counts()
    ax2.cla()
    ax2.set_title(f"Top {TOPN} Categories (last {BAR_SIZE} msgs)")
    ax2.set_xlabel("Category")
    ax2.set_ylabel("Count")
    ax2.grid(True, axis="y", alpha=0.25)
    ax2.bar(labels, values)

    fig.canvas.draw_idle()
    fig.canvas.flush_events()
    plt.pause(0.001)


# ============ PROCESSING ============
def process_one(obj: dict, is_real: bool):
    global _last_real_msg_monotonic, _seen_real, wait_text
    try:
        ts = obj.get("timestamp")
        s  = obj.get("sentiment")
        c  = obj.get("category", "other")
        if ts is None or s is None:
            logger.debug(f"Skipping message missing timestamp/sentiment: {obj}")
            return

        t = parse_ts(ts)
        v = float(s)

        TS.append(t)
        SENT.append(v)
        ROLL.append(v)
        ROLL_AVG.append(sum(ROLL) / len(ROLL))
        CAT_WIN.append(str(c))

        if is_real:
            _last_real_msg_monotonic = time.perf_counter()
            if not _seen_real:
                _seen_real = True
                try:
                    wait_text.set_text("")  # hide "Waiting..." once real data comes
                except Exception:
                    pass

        update_chart()
    except Exception as e:
        logger.error(f"Error processing message: {e}")


# ============ LOOPS ============
def kafka_loop():
    topic = get_topic()
    group = get_group_id()
    logger.info(f"[KAFKA] Topic='{topic}' | Group='{group}' | RollWindow={ROLL_WIN} | BarWindow={BAR_SIZE} | FPS={FPS} | FakeAfter={FAKE_IDLE}s")

    try:
        consumer = create_kafka_consumer(topic, group)
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        logger.error("Is Kafka running? Is KAFKA_SERVER correct in .env?")
        return

    # Prefer poll() to avoid indefinite blocking → lets us inject fallback if idle
    try:
        while True:
            batch = consumer.poll(timeout_ms=500)  # non-blocking wait
            got_any = False
            for _tp, msgs in batch.items():
                for m in msgs:
                    got_any = True
                    obj = normalize_value(m.value)
                    process_one(obj, is_real=True)

            if not got_any:
                # no data this cycle; keep UI responsive
                update_chart()

                # inject synthetic if idle too long
                if FAKE_IDLE > 0 and (time.perf_counter() - _last_real_msg_monotonic) >= FAKE_IDLE:
                    fake = synth_message()
                    logger.debug("Injecting synthetic message (idle fallback).")
                    process_one(fake, is_real=False)
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
    logger.info(f"[FILE] Tailing '{path}' | RollWindow={ROLL_WIN} | BarWindow={BAR_SIZE} | FPS={FPS} | FakeAfter={FAKE_IDLE}s")

    try:
        with path.open("r", encoding="utf-8") as f:
            f.seek(0, os.SEEK_END)
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.05)
                    update_chart()
                    # inject synthetic if file idle
                    if FAKE_IDLE > 0 and (time.perf_counter() - _last_real_msg_monotonic) >= FAKE_IDLE:
                        fake = synth_message()
                        logger.debug("Injecting synthetic message (idle fallback, file mode).")
                        process_one(fake, is_real=False)
                    continue
                try:
                    obj = json.loads(line)
                    process_one(obj, is_real=True)
                except Exception as e:
                    logger.error(f"Bad line skipped: {e}")
    except FileNotFoundError:
        logger.error(f"File not found: {path}. Start the producer or rely on fallback.")
        # Still show synthetic stream so the project isn't empty
        try:
            while True:
                fake = synth_message()
                process_one(fake, is_real=False)
                time.sleep(1.0)
        except KeyboardInterrupt:
            logger.warning("Synthetic loop interrupted by user.")
    except KeyboardInterrupt:
        logger.warning("File tail interrupted by user.")


# ============ MAIN ============
def main():
    global TS, SENT, ROLL, ROLL_AVG, CAT_WIN, ROLL_WIN, HIST_SIZE, BAR_SIZE, TOPN, FPS, FAKE_IDLE
    global _last_real_msg_monotonic, _seen_real, wait_text

    # re-read env in case you tweak values between runs
    ROLL_WIN = get_roll_window()
    HIST_SIZE = get_history_size()
    BAR_SIZE = get_bar_window()
    TOPN = get_topn()
    FPS = get_plot_fps()
    FAKE_IDLE = get_fake_idle_seconds()

    TS = deque(maxlen=HIST_SIZE)
    SENT = deque(maxlen=HIST_SIZE)
    ROLL = deque(maxlen=ROLL_WIN)
    ROLL_AVG = deque(maxlen=HIST_SIZE)
    CAT_WIN = deque(maxlen=BAR_SIZE)

    _last_real_msg_monotonic = time.perf_counter()
    _seen_real = False
    try:
        wait_text.set_text("Waiting for data...")
    except Exception:
        pass

    mode = get_ingest_mode()
    logger.info(f"START project_consumer_kabore | Mode={mode} | Backend={matplotlib.get_backend()}")

    if mode == "file":
        file_loop()
    else:
        kafka_loop()


if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
