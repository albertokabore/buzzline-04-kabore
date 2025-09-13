"""
json_consumer_case.py

Consume JSON messages from a Kafka topic and visualize author counts in real time.

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message after deserialization
{"message": "I love Python!", "author": "Eve"}
"""

# Imports
import os
import json
from collections import defaultdict

from dotenv import load_dotenv
import matplotlib.pyplot as plt

from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Env
load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

# Stream state
author_counts = defaultdict(int)

# Live figure
fig, ax = plt.subplots()
plt.ion()

bars = None                # BarContainer to update in place
author_order: list[str] = []  # stable order of authors

def update_chart():
    """Update the live bar chart of author counts."""
    global bars, author_order

    # keep a stable order and append new authors when they appear
    for a in author_counts.keys():
        if a not in author_order:
            author_order.append(a)

    counts_list = [author_counts[a] for a in author_order]
    x = list(range(len(author_order)))

    # first draw or author set changed
    if bars is None or len(bars) != len(author_order):
        ax.clear()
        bars = ax.bar(x, counts_list, color="skyblue")

        ax.set_xticks(x)
        ax.set_xticklabels(author_order, rotation=45, ha="right")

        ax.set_xlabel("Authors")
        ax.set_ylabel("Message Counts")
        ax.set_title("JSON author counts by Albert Kabore")
        ax.set_ylim(0, max(counts_list) + 1 if counts_list else 1)
        plt.tight_layout()
    else:
        # update bar heights in place
        for rect, h in zip(bars, counts_list):
            rect.set_height(h)
        ax.set_ylim(0, max(counts_list) + 1 if counts_list else 1)

    plt.draw()
    plt.pause(0.01)

def process_message(message) -> None:
    """
    Process a single JSON message from Kafka and update the chart.
    Accepts bytes or str.
    """
    try:
        if isinstance(message, bytes):
            message = message.decode("utf-8")

        logger.debug(f"Raw message: {message}")
        message_dict = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            author = message_dict.get("author", "unknown")
            logger.info(f"Message received from author: {author}")

            author_counts[author] += 1
            logger.info(f"Updated author counts: {dict(author_counts)}")

            update_chart()
            logger.info("Chart updated successfully.")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main() -> None:
    """
    Read topic and group id from env,
    create consumer, poll messages, update chart.
    """
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer for topic '{topic}' and group '{group_id}'")

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'")
    try:
        for record in consumer:
            message_value = record.value
            logger.debug(f"Offset {record.offset}: {message_value!r}")
            process_message(message_value)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()
