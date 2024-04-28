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

# Set environment
config_file = configs
load_dotenv(os.path.join(config_file['python_path'], '.env'))

logs_path = config_file['logs_path']

# Create or get the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging_formater = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')

logging_fh = logging.FileHandler(filename=os.path.join(
    logs_path, 'ingestion.logs'), encoding='utf-8')
logging_fh.setLevel(logging.DEBUG)
logging_fh.setFormatter(logging_formater)

logging_sh = logging.StreamHandler()
logging_sh.setLevel(logging.DEBUG)
logging_sh.setFormatter(logging_formater)

logger.addHandler(logging_fh)
logger.addHandler(logging_sh)


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
        requests.exceptions.MissingSchema : The URL scheme (e.g. http or
            https) is missing.
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
    logger.info("Start ingestion")
    api_url = os.getenv("URL")

    try:
        r = requests.get(api_url, timeout=30)
        r.raise_for_status()
    except requests.exceptions.InvalidURL as err:
        logger.error(str(err))
        raise err
    except requests.exceptions.MissingSchema as err:
        logger.error(str(err))
        raise err
    except requests.exceptions.ConnectTimeout as err:
        logger.error(str(err))
        raise err
    except requests.exceptions.ReadTimeout as err:
        logger.error(str(err))
        raise err
    except requests.exceptions.ConnectionError as err:
        logger.error(str(err))
        raise err
    except requests.exceptions.HTTPError as err:
        logger.error(str(err))
        raise err
    except requests.exceptions.JSONDecodeError as err:
        logger.error(str(err))
        raise err

    data = r.json()['results']
    df = pd.json_normalize(data)
    df['load_date'] = datetime.datetime.now().strftime("%H:%M:%S")

    # Save raw layer
    try:
        file = f"{config_file['raw_path']}{str(uuid.uuid4())}.csv"
        df.to_csv(file, sep=";", index=False)
    except OSError as err:
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

    logger.info("Start preparation")
    try:
        df = pd.read_csv(file, sep=";")
        san = utils.Saneamento(df, config_file)
    except FileNotFoundError as err:
        logger.error(str(err))
        raise err

    logger.info("Rename data")
    san.rename_cols()

    logger.info("Data typing")
    san.tipagem()

    logger.info("Treat str data")
    san.treat_str()

    logger.info("Saving data")
    try:
        san.save_work()
    except mysql.connector.Error as err:
        logger.error(str(err))


if __name__ == '__main__':
    file_name = ingestion()
    preparation(file_name)
