"""
csv_consumer_case.py

Consume JSON messages from a Kafka topic and visualize temperature in real time.

Example Kafka message:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}
"""

#####################################
# Import Modules
#####################################

# Standard Library
import os
import json
from collections import deque

# External
from dotenv import load_dotenv

# Matplotlib for live plotting
import matplotlib.pyplot as plt

# Local utilities
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_stall_threshold() -> float:
    return float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))

def get_rolling_window_size() -> int:
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

#####################################
# Stream State
#####################################

timestamps: list[str] = []
temperatures: list[float] = []

#####################################
# Live Figure
#####################################

fig, ax = plt.subplots()
plt.ion()

# keep and update artists instead of clearing the axes
line = None
stall_marker = None
figure_initialized = False

#####################################
# Stall Detection
#####################################

def detect_stall(rolling_window: deque, window_size: int) -> bool:
    if len(rolling_window) < window_size:
        logger.debug(
            f"Rolling window size {len(rolling_window)} waiting for {window_size}"
        )
        return False
    temp_range = max(rolling_window) - min(rolling_window)
    is_stalled = temp_range <= get_stall_threshold()
    if is_stalled:
        logger.debug(f"Temperature range {temp_range} F stall True")
    return is_stalled

#####################################
# Update Chart
#####################################

def update_chart(rolling_window: deque, window_size: int):
    """Update line and optional stall marker without redrawing the whole plot."""
    global line, stall_marker, figure_initialized

    # first time setup
    if not figure_initialized:
        # main temperature line
        (line,) = ax.plot([], [], label="Temperature")

        # precreate a marker for stall and make it hidden until used
        (stall_marker,) = ax.plot(
            [], [], marker="o", linestyle="None", label="Stall detected", zorder=5
        )

        ax.set_xlabel("Time")
        ax.set_ylabel("Temperature F")
        ax.set_title("Smart Smoker Temperature vs Time by Albert Kabore")
        ax.legend()
        plt.tight_layout()
        figure_initialized = True

    # update main line artist
    line.set_data(timestamps, temperatures)

    # autoscale y from new data
    ax.relim()
    ax.autoscale_view(scalex=True, scaley=True)

    # stall marker on the newest point if stalled
    if len(temperatures) and detect_stall(rolling_window, window_size):
        stall_marker.set_data([timestamps[-1]], [temperatures[-1]])
    else:
        # hide marker by clearing its data
        stall_marker.set_data([], [])

    # draw
    plt.draw()
    plt.pause(0.01)

#####################################
# Process One Message
#####################################

def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    try:
        logger.debug(f"Raw message: {message}")
        data: dict = json.loads(message)

        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed message: {data}")

        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        # update stream state
        rolling_window.append(float(temperature))
        timestamps.append(timestamp)
        temperatures.append(float(temperature))

        # update visualization
        update_chart(rolling_window, window_size)

        if detect_stall(rolling_window, window_size):
            logger.info(
                f"STALL at {timestamp}: Temp near {temperature} F over last {window_size} readings"
            )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

#####################################
# Main
#####################################

def main() -> None:
    logger.info("START consumer")

    # reset state for a clean run
    timestamps.clear()
    temperatures.clear()

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    rolling_window = deque(maxlen=window_size)

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Offset {message.offset} message {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed")

#####################################
# Entrypoint
#####################################

if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
