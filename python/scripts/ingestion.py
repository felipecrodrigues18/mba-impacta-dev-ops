"Script de ingestão e preparação - projeto dataops MBADE04"

# Standard libraries
import os
import uuid
import logging
import datetime

# Third party libraries
import requests
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Repo modules
import utils
from config import configs
from log_config import setup_logging

# Set environment
config_file = configs
logs_path = config_file['logs_path']
setup_logging(os.path.join(logs_path, 'ingestion.logs'))
load_dotenv(os.path.join(config_file['python_path'], '.env'))


def ingestion() -> str:
    """
    Read API data and save raw layer.

    Returns
    -------
    str
        Path to raw layer saved data.

    Raises
    err
        requests.exceptions.InvalidURL : The URL provided was somehow invalid.
        requests.exceptions.ConnectionError : A Connection error occurred.
        requests.exceptions.ConnectTimeout : The request timed out while
            trying to connect to the remote server.
        requests.exceptions.ReadTimeout : The server did not send any data
            in the allotted amount of time.
        requests.exceptions.HTTPError : An HTTP error occurred.
        requests.exceptions.JSONDecodeError : Couldn't decode the text
            into json
        OSError: Cannot save file into a non-existent directory.
    """
    logging.info("Start ingestion")
    api_url = os.getenv("URL")

    try:
        r = requests.get(api_url, timeout=30)
        r.raise_for_status()
    except requests.exceptions.InvalidURL as err:
        logging.error(err)
        raise err
    except requests.exceptions.ConnectTimeout as err:
        logging.error(err)
        raise err
    except requests.exceptions.ReadTimeout as err:
        logging.error(err)
        raise err
    except requests.exceptions.ConnectionError as err:
        logging.error(err)
        raise err
    except requests.exceptions.HTTPError as err:
        logging.error(err)
        raise err
    except requests.exceptions.JSONDecodeError as err:
        logging.error(err)
        raise err

    data = r.json()['results']
    df = pd.json_normalize(data)
    df['load_date'] = datetime.datetime.now().strftime("%H:%M:%S")

    # Save raw layer
    try:
        file = f"{config_file['raw_path']}{str(uuid.uuid4())}.csv"
        df.to_csv(file, sep=";", index=False)
    except OSError as err:
        logging.error(str(err))
        raise err

    return file


def preparation(file: str):
    """
    Data treatment and database ingestion pipeline.

    Parameters
    ----------
    file : str
        Path to file in raw layer.
    """

    logging.info("Start preparation")
    try:
        df = pd.read_csv(file, sep=";")
        san = utils.Saneamento(df, config_file)
    except FileNotFoundError as err:
        logging.error(str(err))
        raise err

    logging.info("Rename data")
    san.select_rename()

    logging.info("Data typing")
    san.tipagem()

    logging.info("Saving data")
    try:
        san.save_work()
    except mysql.connector.Error as err:
        logging.error(str(err))


if __name__ == '__main__':
    file_name = ingestion()
    preparation(file_name)
