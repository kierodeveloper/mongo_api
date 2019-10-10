#-*- coding: utf-8 -*-
"""==================== * modules * ======================"""
import pyodbc
import json 
from json import dumps
import re
import os
import pymongo
from datetime import datetime
from argparse import ArgumentParser
import sys
"""==================== * end modules * ======================"""
"""==================== * import app-controllers * ======================"""
from app.write_data_with_filename import data_with_filename
"""==================== * end import app-controllers * ======================"""
"""==================== * import app-modules * ======================"""
from scripts.insert import controllers
"""==================== * end import app-modules * ======================"""



now = datetime.now() # current date and time
date_time = now.strftime("%m%d%Y%H%M%S")


parser = ArgumentParser()
parser.add_argument("-in", "--insert_in", dest="insert_in",help="exec save in database, select 'm' for mongodb and 'e' for elasticsearch", required=True)
parser.add_argument("-t", "--table", dest="name_table",help="file name", required=True)
parser.add_argument("-top", "--top", type=int, dest="top",help="select top", required=False)
parser.add_argument("-d", "--delimiter", dest="delimiter",help="delimiter row", required=True)


args = parser.parse_args()
def validate(insert_in,name_table,delimiter):
    correct_value = ['m','e']
    if insert_in in correct_value:
        print('ejecutando ...')
    else:
        print('"insert_in" is not defined correctly, use either of the two (m for "mongoDB" or e for "elasticsearch")')
        sys.exit()


if args.insert_in and args.name_table and args.delimiter:
    validate(args.insert_in,args.name_table,args.delimiter)
    table = data_with_filename(table_name=args.name_table,delimiter=args.delimiter,top=args.top)
    number_of_rows = table.create_datafeed()
    controllers()._insert_data(delimiter=(args.delimiter),insert_in=args.insert_in,number_of_rows = number_of_rows)



