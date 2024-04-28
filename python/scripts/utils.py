
"""
ETL pipeline process for type treatment, column renaming, data treatment
and saving treated data to work layer in MySQL database.
"""

# Standard libraries
import datetime
import unicodedata

# Third party libraries
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine


class Saneamento:
    """
    Data treatment and ingestion pipeline.
    """

    def __init__(self, data: pd.DataFrame, configs: dict):
        """Class constructor"""
        self.data = data

        try:
            self.metadado = pd.read_excel(configs["meta_path"])
        except FileNotFoundError as err:
            raise err
        self.len_cols = max(list(self.metadado["id"]))
        self.colunas = list(self.metadado['nome_original'])
        self.colunas_new = list(self.metadado['nome'])
        self.path_work = configs["work_path"]
        self.lower_case_cols = list(
            self.metadado[self.metadado['lower_case'] == 1]['nome'])
        self.ascii_cols = list(
            self.metadado[self.metadado['convert_ascii'] == 1]['nome'])

    def rename_cols(self):
        """Rename columns"""
        self.data = self.data.loc[:, self.colunas]
        for i in range(self.len_cols):
            self.data.rename(
                columns={self.colunas[i]: self.colunas_new[i]}, inplace=True)

    def tipagem(self):
        """Treat data type"""
        for col in self.colunas_new:
            tipo = self.metadado.loc[
                self.metadado['nome'] == col]['tipo'].item()
            if tipo == "int":
                tipo = self.data[col].astype(int)
            elif tipo == "float":
                self.data[col].replace(",", ".", regex=True, inplace=True)
                self.data[col] = self.data[col].astype(float)
            elif tipo == "date":
                self.data[col] = pd.to_datetime(
                    self.data[col]).dt.strftime('%Y-%m-%d')

    def treat_str(self):
        """
        Convert str to upper, lower cases and convert special characters to
        ascii convetion.
        """
        # Treat lower case columns
        self.data[self.lower_case_cols] = self.data[
            self.lower_case_cols].map(lambda x: x.lower())

        # Convert special characters to ascii
        self.data[self.ascii_cols] = self.data[
            self.ascii_cols].map(
                lambda x: unicodedata.normalize('NFD', x).encode(
                    'ascii', 'ignore').decode())

    def save_work(self):
        """Save treated data to database"""
        self.data['load_date'] = datetime.datetime.today().strftime(
            '%Y-%m-%d %H:%M:%S')

        try:
            con = mysql.connector.connect(
                user='root', password='root', host='mysql',
                port="3306", database='db')
            engine = create_engine(
                "mysql+mysqlconnector://root:root@mysql/db")
            self.data.to_sql(
                'cadastro', con=engine, if_exists='append', index=False)
        except mysql.connector.Error as err:
            raise err
        finally:
            con.close()
