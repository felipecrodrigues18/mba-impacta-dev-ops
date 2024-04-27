"""
LOG HANDLING MODULE

DESCRIPTION
    Provides a function that allow handling logging messages and
    error handling log creation.

FUNCTIONS:
    setup_logging(file_name)
        Saves logging file with severity and message.

EXAMPLE:
    handler(file_name)
"""

# Standard libraries
import os
import logging


def setup_logging(file_name: str):
    """
    Create loggin file and write down loggig messages based on severity.

    Parameters
    ----------
    file_name : str
        Loggin file path.
    """

    logging.basicConfig(
        filename=file_name,
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.DEBUG
    )
