#-*- coding: utf-8 -*-
import pyodbc
import json 
from json import dumps
import re
import os
import pymongo
from datetime import datetime
# Import Elasticsearch package 
from elasticsearch import Elasticsearch 
from elasticsearch.helpers import bulk
import glob
from json import dumps
import math
now = datetime.now() # current date and time
date_time = now.strftime("%m%d%Y%H%M%S")
cwd = os.getcwd()
# try: 
#     conn_mongo = pymongo.MongoClient("mongodb://172.17.0.3:27017/")
#     print("Connected successfully!!!") 
# except: 
#     print("Could not connect to MongoDB") 

# db = conn_mongo.tbl_random_products
# collection = db.products_by_category

# datafeed = 'content/products_{date_time}.txt'.format(date_time=date_time)
save_err = open('storage/err.txt','a')
# save_products = open(datafeed,'a')


conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=204.141.52.148;DATABASE=DBKiero_Productos;UID=MachineBaseConnect3651;PWD=H1#KotS(xh5nF+tGv')


def generate(conn):
    with conn:
        query_count = """SELECT count(*) FROM tbl_Productos"""
        count_crsr = conn.execute(query_count)
        count_rows = count_crsr.fetchone()

    with conn:
        query = """SELECT top(50) * FROM tbl_Productos"""
        crsr = conn.execute(query)
        rows = crsr.fetchall()

    number_of_files = math.ceil(50 / 5)


    while lote < number_of_files:
        


    # if stop < number_of_files:
    #         print('crear archivo')
    #         print(rows[count][0])
    #         aux +=1
    #         count +=1
    #     lote += 1
    #     stop = aux + 5




    # number_of_rows = math.ceil(50 / 5)
    # print(number_of_rows)                
    # lot = 0
    # aux = 0 
    # stop = aux + 5
    # filename = 0
    # print('cargando ...')
    # while lot < number_of_rows:
    #     full_path = cwd+"/content/data_json/save_rows"+str(filename)+'.txt'
    #     save_json = open(full_path,'a')
        
    #     if aux < stop:
    #         for iteration in rows:
    #             save_json.writelines(str(iteration)+'\n')
    #             aux += 1

    #     save_json.close
    #     lot += 1
    #     filename += 1
    #     stop = aux + 5

    

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Conectado')
        return(_es)
    else:
        return('Awww it could not connect!')
    return _es

def search_products(query):
    res= _es.search(index='search_products',body={
        'query':{
            'bool':{
                'must':[{
                        'match':{
                            'Titulo':query
                        }
                    }]
            }
        }
    })

    return (res['hits']['hits'])

def insert_datafeed(datafeed):
    for row in datafeed:
        yield {
            "_index": "search_products",
            "_type": "products",
            "_id": row.split('|')[0],
            "_source":{
                'Producto_Id':row.split('↨')[000],
                'Categoria_id':row.split('↨')[000],
                'Titulo':row.split('↨')[000],
                'Descripcion':row.split('↨')[000],
                'Precio':row.split('↨')[000],
                'Precio_Descuento':row.split('↨')[000],
                'Stock_Actual':row.split('↨')[000],
                'Stock_Limite':row.split('↨')[000],
                'Estado':row.split('↨')[000],
                'Referencia_Amazon':row.split('↨')[000],
                'Peso':row.split('↨')[000],
                'Alto':row.split('↨')[000],
                'Largo':row.split('↨')[000],
                'Ancho':row.split('↨')[000],
                'Color':row.split('↨')[000],
                'Talla':row.split('↨')[000],
                'Porcentaje':row.split('↨')[000],
                'Precio_cop':row.split('↨')[000],
                'Creado_Por':row.split('↨')[000],
                'Modificado_Por':row.split('↨')[000],
                'Fecha_Creacion':row.split('↨')[000],
                'Fecha_Actualizacion':row.split('↨')[000],
                'codigo':row.split('↨')[000],
                'breadcrum':row.split('↨')[000],
                'GUID':row.split('↨')[000],
                'SKU':row.split('↨')[000],
                'Informacion':row.split('↨')[000],
                'Imagenes_1':row.split('↨')[000],
                'Imagenes_2':row.split('↨')[000],
                'Imagenes_3':row.split('↨')[000],
                'Imagenes_4':row.split('↨')[000],
                'Imagenes_5':row.split('↨')[000],
                'Imagenes_6':row.split('↨')[000],
                'Imagenes_7':row.split('↨')[000]
            },
        }
        

_es = connect_elasticsearch()
generate(conn)

# insert_datafeed(json_products,datafeed)

# print(search_products('coches para bebes'))

# with open('storage/result.json','a') as file:
#     file.writelines(dumps(search_products('coches para bebes'), indent=4))
# _es = connect_elasticsearch()
# _es.index(index='search_products',doc_type='product',body=json_products)
# res= _es.search(index='search_products',body={'query':{'match':{'Product_id':'None'}}})
# print(res)

# res = es.index(index='megacorp',doc_type='employee',id=1,body=e1)

# ==================================================0inseert =======================================

# datafeeds = glob.glob("/home/root-33/Documentos/search/feeds/*.txt")
# for data in datafeeds:
#     print(insert_datafeed(datafeeds))
    # datafeed = open(data,'r')
    # bulk(_es, insert_datafeed(datafeed),request_timeout=30)


# =========================================================================================================

# res= _es.search(index='search_products',body={
#         'query':{
#             'bool':{
#                 'must':{
#                     'match':{
#                         'Titulo':'zapato'
#                     }
#                 },
#                 "filter":{
#                     "range":{
#                         "Category_id":{
#                             "gt":29707
#                         }
#                     }
#                 }
#             }
#         }
#     })

# print( res['hits']['hits'])

# =========================================================================================================
# doc = {
#         'size' : 10000,
#         'query': {
#             'match_all' : {}
#        }
#    }
# res = _es.search(index='test', body=doc)
# print(res)


