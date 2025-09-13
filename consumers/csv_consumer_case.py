"""
csv_consumer_case.py

Consume JSON messages from a Kafka topic and visualize temperature in real time.

Example Kafka message:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}
"""

# ---------- Imports ----------
import os
import json
from collections import deque
from datetime import datetime, timezone
import matplotlib.dates as mdates

from dotenv import load_dotenv
import matplotlib.pyplot as plt

from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# ---------- Env ----------
load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_stall_threshold() -> float:
    # Max allowed variation over the rolling window to consider it a stall
    return float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))

def get_rolling_window_size() -> int:
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

# ---------- Helpers ----------
def parse_iso_ts(ts) -> datetime:
    """Parse ISO timestamps robustly (supports 'Z' or no zone)."""
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(float(ts), tz=timezone.utc)
    s = str(ts)
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        # Fallback without microseconds
        return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")

# ---------- Stream state ----------
times_dt = []      # datetime objects for x-axis
temperatures = []  # floats for y-axis

# ---------- Live figure ----------
fig, ax = plt.subplots()
plt.ion()
plt.show(block=False)  # make the window appear immediately

line = None          # Line2D for the temperature curve
stall_marker = None  # Marker for stall indication
figure_initialized = False

# ---------- Stall detection ----------
def detect_stall(rolling_window: deque, window_size: int) -> bool:
    if len(rolling_window) < window_size:
        logger.debug(f"Rolling window {len(rolling_window)}/{window_size}")
        return False
    temp_range = max(rolling_window) - min(rolling_window)
    is_stalled = temp_range <= get_stall_threshold()
    if is_stalled:
        logger.debug(f"Temperature range {temp_range:.3f} °F -> STALL")
    return is_stalled

# ---------- Chart update ----------
def update_chart(rolling_window: deque, window_size: int):
    """Update line & optional stall marker without clearing the axes."""
    global line, stall_marker, figure_initialized

    if not figure_initialized:
        (line,) = ax.plot([], [], label="Temperature")
        (stall_marker,) = ax.plot(
            [], [], marker="o", linestyle="None", label="Stall detected", zorder=5
        )

        ax.set_title("Smart Smoker Temperature vs Time by Albert Kabore")
        ax.set_xlabel("Time")
        ax.set_ylabel("Temperature (°F)")

        # Proper datetime axis formatting
        locator = mdates.AutoDateLocator()
        formatter = mdates.ConciseDateFormatter(locator)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)

        ax.legend()
        plt.tight_layout()
        figure_initialized = True

    # Update main line
    line.set_data(times_dt, temperatures)

    # Autoscale to new data when present
    if temperatures:
        ax.relim()
        ax.autoscale_view(scalex=True, scaley=True)

    # Show/hide stall marker at newest point
    if temperatures and detect_stall(rolling_window, window_size):
        stall_marker.set_data([times_dt[-1]], [temperatures[-1]])
    else:
        stall_marker.set_data([], [])

    plt.draw()
    plt.pause(0.01)

# ---------- Process one message ----------
def process_message(message, rolling_window: deque, window_size: int) -> None:
    try:
        # Kafka may give bytes; normalize to str
        if isinstance(message, bytes):
            message = message.decode("utf-8")

        logger.debug(f"Raw message: {message}")
        data: dict = json.loads(message)

        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Update stream state (parse ts to datetime, cast temp to float)
        t = parse_iso_ts(timestamp)
        v = float(temperature)

        rolling_window.append(v)
        times_dt.append(t)
        temperatures.append(v)

        update_chart(rolling_window, window_size)

        if detect_stall(rolling_window, window_size):
            logger.info(
                f"STALL at {t.isoformat()}: Temp near {v:.1f} °F over last {window_size} readings"
            )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

# ---------- Main ----------
def main() -> None:
    logger.info("START consumer")

    # Clean run
    times_dt.clear()
    temperatures.clear()

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    rolling_window = deque(maxlen=window_size)

    # Force an initial draw so the window appears before first message
    update_chart(rolling_window, window_size)

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'")
    try:
        for record in consumer:
            message_value = record.value  # bytes or str
            logger.debug(f"Offset {record.offset} -> {message_value!r}")
            process_message(message_value, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed")

# ---------- Entrypoint ----------
if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
