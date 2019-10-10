import pymongo
from tqdm import tqdm
from json import dumps
myclient = pymongo.MongoClient("mongodb://172.17.0.2:27017/")
db = myclient["new_products"]
collection = db["products"]
import pyodbc
import json 
import re
import os
import pymongo



db_category = myclient["kiero"]
collection_category = db_category["products2"]

print(collection_category.find().count())



# collection.drop()

# cursor = collection.find_one()
# print(cursor)


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

