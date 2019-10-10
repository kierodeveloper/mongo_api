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

