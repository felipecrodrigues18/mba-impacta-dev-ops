"""
LOG HANDLING MODULE

DESCRIPTION
    Provides a function that allow handling logging messages and
    error handling log creation.

FUNCTIONS:
    setup_logging(filename)
        Saves logging file according to
        message and severity info path filename.

EXAMPLE:
    message: "Get API data"
    user: "John Snow"
    stage: "Data fetching"
    log_file_path: "logs.txt"
        handler(message, user, stage, log_file_path)
"""

# Standard libraries
import os
import logging


def setup_logging(filename: str):
    """
    Create or add error handling messages to file in log directory.

    Parameters
    ----------
    filename : str
        Logging path. Must be provided explicitly as a keyword argument.
    """

    log_dir = os.path.dirname(filename)
    if not os.path.isdir(log_dir):
        exception_error = FileNotFoundError(
            f"Directory {log_dir} not found.")
        raise exception_error

    logging.basicConfig(
        filename=filename,
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
