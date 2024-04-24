
# Standard libraries
import os
import datetime

# Third party libraries
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine


class Saneamento:


    def __init__(self, data, configs):
        self.data = data
        self.metadado =  pd.read_excel(configs["meta_path"])
        self.len_cols = max(list(self.metadado["id"]))
        self.colunas = list(self.metadado['nome_original'])
        self.colunas_new = list(self.metadado['nome'])
        self.path_work = configs["work_path"]        


    def select_rename(self):
        """_summary_
        """        
        self.data = self.data.loc[:, self.colunas] 
        for i in range(self.len_cols):
            self.data.rename(
                columns={self.colunas[i]:self.colunas_new[i]}, inplace = True)


    def tipagem(self):
        """_summary_
        """        
        for col in self.colunas_new:
            tipo = self.metadado.loc[
                self.metadado['nome'] == col]['tipo'].item()
            if tipo == "int":
                tipo = self.data[col].astype(int)
            elif tipo == "float":
                self.data[col].replace(",", ".", regex=True, inplace = True)
                self.data[col] = self.data[col].astype(float)
            elif tipo == "date":
                self.data[col] = pd.to_datetime(
                    self.data[col]).dt.strftime('%Y-%m-%d')


    def save_work(self):
        """_summary_

        Raises
        ------
        err
            _description_
        """        
        self.data['load_date'] = datetime.datetime.today().strftime(
            '%Y-%m-%d %H:%M:%S')

        try:
            con = mysql.connector.connect(
                user='root', password='root', host='mysql',
                port="3306", database='db')
            engine  = create_engine(
                "mysql+mysqlconnector://root:root@mysql/db")
            self.data.to_sql(
                'cadastro', con=engine, if_exists='append', index=False)
        except mysql.connector.Error as err:
            raise err
        finally:
            con.close()
