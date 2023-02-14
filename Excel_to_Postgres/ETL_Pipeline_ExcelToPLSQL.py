from sqlalchemy import create_engine
import pyodbc
import psycopg2
import pandas as pd
import os

user = 'admin'
password = 'admin'
host = 'localhost'
port = 5432
databse = 'postgres' 
dir = "/Users/vaibhavivichare/Documents/Other/ETL/Excel_to_Postgres"

def extract():
    try:
        dir_list = os.listdir(dir)

        for filename in dir_list:

            file_wo_ext = os.path.splitext(filename)[0]

            if filename.endswith('.xlsx'):
                file_path = os.path.join(dir, filename)
                df = pd.read_excel(file_path)
                load(df, file_wo_ext)

    except Exception as e:

        print("Data Extract Error", str(e))

def load(df , tbl):
    try:

        connection = create_engine(url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(user,password,host,port,databse))

        df.to_sql(f"stg_{tbl}", connection, if_exists = 'replace', index = 'False')
    
    except Exception as e:

        print("Loading error", str(e))

df = extract()
