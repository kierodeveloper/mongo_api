import pymongo
from tqdm import tqdm
from json import dumps
myclient = pymongo.MongoClient("mongodb://172.17.0.2:27017/")
db = myclient["products_v2"]
collection = db["products"]
import pyodbc
import json 
import re
import os
import pymongo

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=204.141.52.148;DATABASE=DBKiero_Productos;UID=MachineBaseConnect3651;PWD=H1#KotS(xh5nF+tGv')

def upsert_mongo(rows):
    for row in tqdm(rows):

        # if row[0] == 'Producto_Id':
        #     print(row)
        yield {
            'Producto_Id':row[0],
            'Categoria_id':row[1],
            'Titulo':row[2],
            'Descripcion':row[3],
            'Precio_Descuento':row[4],
            'Estado':row[5],
            'Porcentaje':row[6],
            'Informacion':row[7],
            'Imagenes_1':row[8]
        }
        
        # count = collection.count_documents({'Producto_Id':build_json['Producto_Id']})

        # #     if count > 0:
        # #         for producto in cursor:
        # #             print(producto['Producto_Id'])
        # #     else:
        # #         print('hola')


def create_datafeed(rows):
    for row in tqdm(rows):

        # if row[0] == 'Producto_Id':
        #     print(row)
        build_json = {
            'Producto_Id':row[0],
            'Categoria_id':row[1],
            'Titulo':row[2],
            'Descripcion':row[3],
            'Precio_Descuento':row[4],
            'Estado':row[5],
            'Porcentaje':row[6],
            'Informacion':row[7],
            'Imagenes_1':row[8]
        }

        with open('content/new_datafeed.txt') as file:
            file.write(build_json)    

def create_collection_category(rows):
    for row in tqdm(rows):
        yield {
            'Categoria_id':row[0]
        }

#usted ejecuta y me envia imagenes
with conn:
    query = """SELECT Producto_Id, Categoria_id, Titulo, Descripcion, Precio_Descuento, Estado, Porcentaje, Informacion, Imagenes_1 FROM tbl_Productos where Estado = 1"""
    crsr = conn.execute(query)
    rows = crsr.fetchall()


# cursor = collection.insert_many(upsert_mongo(rows))
create_datafeed(rows)

# cursor = collection.insert_many(create_collection_category(rows))
# cursor = collection.drop()


# if cursor:
#     for producto in cursor:
#         print(producto['Producto_Id'])
# else:
#     print('hola')

# if collection.find({'Producto_Id': { "$in": '1'}}).count() > 0:
#     print(hola)
# else:
#     print('hola')
# for producto in cursor:
#     if producto['Producto_Id'] > 0:
#         print(producto['Producto_Id'])
#     else:
#         print('hola')