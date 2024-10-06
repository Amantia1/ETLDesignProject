import logging

def setup_logging(log_file='etl_logger.log'):
    logger = logging.getLogger("ETL Logger")
    logger.setLevel(logging.INFO)

    # Create a file handler for logging to a file
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)

    # Create a console handler for logging to the console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Create a formatter and set it for both handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger