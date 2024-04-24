"Script de ingestão e preparação - projeto dataops MBADE04"

# Standard libraries
import os
import uuid
import datetime
import logging

# Third party libraries
import requests
import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout
import mysql.connector

# Repo modules
import utils
from config import configs
from log_config import setup_logging

config_file = configs
logs_path = config_file['logs_path']
load_dotenv(f"{config_file['repo_path']}/.env")
setup_logging(os.path.join(logs_path, 'ingestion.logs'))


def ingestion() -> str:
    """_summary_

    Returns
    -------
    str
        _description_
    """
    logging.info("Start ingestion")
    api_url = os.getenv('URL')

    try:
        response = requests.get(api_url, timeout=30).json()
        data = response['results']
    except ReadTimeout as err:
        logging.error(str(err))
        raise err

    df = pd.json_normalize(data)
    df['load_date'] = datetime.datetime.now().strftime("%H:%M:%S")
    file = f"{config_file['raw_path']}{str(uuid.uuid4())}.csv"
    df.to_csv(file, sep=";", index=False)
    return file


def preparation(file: str):
    """_summary_

    Parameters
    ----------
    file : str
        _description_
    """

    logging.info("Start preparation")
    try:
        df = pd.read_csv(file, sep=";")
    except FileNotFoundError as err:
        logging.error(str(err))
        raise err
        
    san = utils.Saneamento(df, config_file)

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
