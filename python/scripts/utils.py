import os
import pandas as pd
import datetime
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
        self.data = self.data.loc[:, self.colunas] 
        for i in range(self.len_cols):
            self.data.rename(columns={self.colunas[i]:self.colunas_new[i]}, inplace = True)

    def tipagem(self):
        for col in self.colunas_new:
            tipo = self.metadado.loc[self.metadado['nome'] == col]['tipo'].item()
            if tipo == "int":
                tipo = self.data[col].astype(int)
            elif tipo == "float":
                self.data[col].replace(",", ".", regex=True, inplace = True)
                self.data[col] = self.data[col].astype(float)
            elif tipo == "date":
                self.data[col] = pd.to_datetime(self.data[col]).dt.strftime('%Y-%m-%d')
    
    def save_work(self):
        self.data['load_date'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        con = mysql.connector.connect(
            user='root', password='root', host='mysql', port="3306", database='db')
        
        print("DB connected")

        engine  = create_engine("mysql+mysqlconnector://root:root@mysql/db")
        self.data.to_sql('cadastro', con=engine, if_exists='append', index=False)
        con.close()


def error_handler(exception_error, stage):
    
    log = [stage, type(exception_error).__name__, exception_error,
           datetime.datetime.now()]
    logdf = pd.DataFrame(log).T
    
    if not os.path.exists("logs_file.txt"):
        logdf.columns = ['stage', 'type', 'error', 'datetime']
        logdf.to_csv("logs_file.txt", index=False,sep = ";")
    else:
        logdf.to_csv("logs_file.txt", index=False, mode='a',
                     header=False, sep = ";")