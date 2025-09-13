"""
basic_json_consumer_case.py

Read a JSON formatted file as it is being written.

Example JSON message:
{"message": "I just saw a movie! It was amazing.", "author": "Eve"}
"""

#####################################
# Import Modules
#####################################

# Standard Library
import json
import os  # for file operations
import sys  # to exit early
import time
import pathlib
from collections import defaultdict  # data structure for counting author occurrences

# Matplotlib for live plotting
import matplotlib.pyplot as plt

# Local
from utils.utils_logger import logger

#####################################
# Set up Paths read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("buzz_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures
#####################################

author_counts = defaultdict(int)

#####################################
# Set up live visuals
#####################################

fig, ax = plt.subplots()
plt.ion()  # interactive mode for live updates

# keep bar artists and a stable author order for smooth updates
bars = None
author_order = []

#####################################
# Define an update chart function for live plotting
# This runs every time a new message is processed
#####################################

def update_chart():
    """Update the live chart with the latest author counts."""
    global bars, author_order

    # keep a stable order and append new authors when they appear
    for a in author_counts.keys():
        if a not in author_order:
            author_order.append(a)

    counts_list = [author_counts[a] for a in author_order]
    x = list(range(len(author_order)))

    if bars is None or len(bars) != len(author_order):
        # first draw or author set changed
        ax.clear()
        bars = ax.bar(x, counts_list, color="green")

        # set matching ticks and labels to avoid warnings
        ax.set_xticks(x)
        ax.set_xticklabels(author_order, rotation=45, ha="right")

        ax.set_xlabel("Authors")
        ax.set_ylabel("Message Counts")
        ax.set_title("Author message counts by Albert Kabore")
        ax.set_ylim(0, max(counts_list) + 1 if counts_list else 1)
        plt.tight_layout()
    else:
        # update existing bar heights in place
        for rect, h in zip(bars, counts_list):
            rect.set_height(h)
        ax.set_ylim(0, max(counts_list) + 1 if counts_list else 1)

    plt.draw()
    plt.pause(0.01)

#####################################
# Process Message Function
#####################################

def process_message(message: str) -> None:
    """
    Process a single JSON message and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        logger.debug(f"Raw message: {message}")
        message_dict: dict = json.loads(message)
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

#####################################
# Main Function
#####################################

def main() -> None:
    """
    Main entry point for the consumer.
    Monitors a file for new messages and updates a live chart.
    """
    logger.info("START consumer.")

    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as file:
            file.seek(0, os.SEEK_END)
            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                line = file.readline()

                if line.strip():
                    process_message(line)
                else:
                    logger.debug("No new messages. Waiting...")
                    time.sleep(0.5)
                    continue

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
