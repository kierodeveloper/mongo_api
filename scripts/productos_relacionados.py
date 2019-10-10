#-*- coding: utf-8 -*-
import pyodbc
import json 
from json import dumps
import re
import os, sys
import pymongo
from tqdm import tqdm
from datetime import datetime
#nlp = spacy.load('en_core_web_sm')
now = datetime.now() # current date and time
date_time = now.strftime("%m%d%Y%H%M%S")
try: 
    conn_mongo = pymongo.MongoClient("mongodb://172.17.0.2:27017/")
    print("Connected successfully!!!") 
except: 
    print("Could not connect to MongoDB") 

db = conn_mongo.kiero
collection = db.products2

kiero_categories = conn_mongo.kiero
collection_categories = kiero_categories.publies_categories

datafeed = 'content/categories_{date_time}.txt'.format(date_time=date_time)

save_categories = open(datafeed,'a')

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=204.141.52.148;DATABASE=DBKiero_Productos;UID=MachineBaseConnect3651;PWD=H1#KotS(xh5nF+tGv')

def _split():
    pass

def create_random_categories():
    with open('content/categories.txt','r') as file:
        for i in file:
            collection_categories.insert_one(json.loads(i))

def generate_json(rows):
    for item in rows:
        yield {
            'Producto_Id':str(item[0]),
            'Categoria_id':str(item[1]),
            'Titulo':str(item[2]),
            'Descripcion':str(item[3]),
            'Precio_Descuento':str(item[4]),
            'Precio_cop':str(item[5]), 
            'Estado':str(item[6]),
            'Porcentaje':str(item[7]),
            'Informacion':str(item[8]),
            'Imagenes_1':str(item[9])
        }

# collection.drop()
# collection_categories.drop()
# sys.exit()

def insert_products():
    #seleccionar categorias con mas de 4 productos
    with conn:
        query = """select id, name from public_categories where quantity_products_active > 8"""
        cat_crsr = conn.execute(query)
        categorias = cat_crsr.fetchall()

    for row in tqdm(categorias):
        with conn:
            query_products = """select top(400) Producto_Id,Categoria_id,Titulo,Descripcion,Precio_Descuento,Precio_cop,Estado,Porcentaje,Informacion,Imagenes_1 from tbl_Productos where Categoria_id = {} and Estado = 1 """.format(row[0])
            crsr = conn.execute(query_products)
            productos = crsr.fetchall()

        json_category = {'category_id':row[0]}

        if len(productos) > 12:
            save_categories.write(str(json_category) + '\n')
            for item in tqdm(productos, ascii=True, desc='write products in the category of {}'.format(row[1])):
                build = {
                    'Producto_Id':str(item[0]),
                    'Categoria_id':str(item[1]),
                    'Titulo':str(item[2]),
                    'Descripcion':str(item[3]),
                    'Precio_Descuento':str(item[4]),
                    'Precio_cop':str(item[5]), 
                    'Estado':str(item[6]),
                    'Porcentaje':str(item[7]),
                    'Informacion':str(item[8]),
                    'Imagenes_1':str(item[9])
                }
                collection.insert_one(build)
        else:
            continue

insert_products()