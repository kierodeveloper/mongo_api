import pyodbc
import json 
from json import dumps
import re
import os,sys
import pymongo
import pandas as pd
import csv
from datetime import datetime
import configparser
from tqdm import tqdm
# from mysql.connector import (connection)
# import MySQLdb 
import pyodbc
config = configparser.ConfigParser()
config.read('.conf')
configurate = config['DATABASE']

DB_CONNECTION = str(configurate.get('DB_CONNECTION'))
DB_DRIVER = str(configurate.get('DB_DRIVER'))
DB_HOST = str(configurate.get('DB_HOST'))
DB_DATABASE = str(configurate.get('DB_DATABASE'))
DB_USERNAME = str(configurate.get('DB_USERNAME'))
DB_PASSWORD = str(configurate.get('DB_PASSWORD'))


class data_with_filename:
    def __init__(self,table_name,delimiter,top=None):
        self.__table_name = table_name
        self.__delimiter = delimiter
        self.__top = top
        pass

    def _check_folder_content(self):
        try:
            cwd = os.getcwd()
            list_of_files = os.listdir('content/data_tables/')
            full_path = [cwd+"/content/data_tables/{0}".format(x) for x in list_of_files]
            young_file = max(full_path, key=os.path.getctime) if full_path else None
            if young_file:
                return(young_file)
        except Exception as err:
            print(err)
            sys.exit()

    def _delete_older_file(self):
        try:
            cwd = os.getcwd()
            list_of_files = os.listdir('content/data_tables/')
            full_path = [cwd+"/content/data_tables/{0}".format(x) for x in list_of_files]
            oldest_file = min(full_path, key=os.path.getctime) if full_path else None
            if oldest_file:
                if oldest_file != self._check_folder_content():
                    os.remove(oldest_file)
        except Exception as err:
            print(err)
            sys.exit()
    
    def _split_file(self):
        young_file = self._check_folder_content()
        os.system('rm -r content/split_files/*')
        execute = 'split -l 1000 '+ young_file + ' content/split_files/{}'.format(self.__table_name)
        os.system(execute)

    def create_datafeed(self):

        try:
            now = datetime.now() # current date and time
            date_time = now.strftime("%m%d%Y%H%M%S")

            datafeed = ('content/data_tables/{table_name}_{date_time}.txt'.format(date_time=date_time,table_name=self.__table_name)).replace(' ','_')

            save_data = open(datafeed,'a')

            conn = pyodbc.connect('DRIVER={'+DB_DRIVER+'};SERVER='+DB_HOST+';DATABASE='+DB_DATABASE+';UID='+DB_USERNAME+';PWD='+DB_PASSWORD)
            cursor = conn.cursor()
            
            if self.__top is None:
                #query = 'select Producto_Id,Categoria_id,Titulo,Precio_Descuento,Estado,Porcentaje,Precio_cop,Imagenes_1,Relevancia,Imagenes_L,Imagenes_D,Imagenes_W from {} where Estado = 1'.format(self.__table_name)
                query = 'select p.Producto_Id, p.Categoria_id,p.Titulo, pd.Titulo as Fake, p.Estado, p.Precio_cop, p.Imagenes_1, p.Imagenes_L, p.Imagenes_D, c.Relevancia as Relevancia_cat, p.Relevancia as Relevancia_pro, c.parent_path from {} p inner join tbl_producto_busqueta pd on p.Producto_Id = pd.id inner join public_categories c on pd.id_categoria = c.id where p.Estado = 1'.format(self.__table_name)
                cursor.execute(query)
                writer = csv.writer(save_data, delimiter = str(self.__delimiter), quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0] for col in cursor.description)
            else:
                query = 'select top('+str(self.__top)+') * from {} where Estado = 1'.format(self.__table_name)
                cursor.execute(query)
                writer = csv.writer(save_data, delimiter = str(self.__delimiter), quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0] for col in cursor.description)
            
            print(query)
            count = 0
            for row in tqdm(cursor, ascii=True, desc='Rows extracted'):
                writer.writerow(row)
                count += 1
            self._delete_older_file()
            self._split_file()
            return count
        except Exception as err:
            print(err)
            sys.exit()
            

