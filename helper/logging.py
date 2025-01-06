import logging
from pathlib import Path
import sys

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Log both INFO and ERROR messages

# formatter with a timestamp
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# stream handler to print log messages to command line
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)


# file handler to write log messages text file
log_file = Path(__file__).parent.parent / "logfile.txt"
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(stream_handler)
logger.addHandler(file_handler)
