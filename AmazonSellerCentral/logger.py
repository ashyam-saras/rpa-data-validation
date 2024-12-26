import logging
import sys

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Log both INFO and ERROR messages

#formatter with a timestamp
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

#stream handler to print log messages to command line
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

#file handler to write log messages text file
file_handler = logging.FileHandler('logfile.txt')
file_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(stream_handler)
logger.addHandler(file_handler)
