import pymongo
from tqdm import tqdm
import sys
myclient = pymongo.MongoClient("mongodb://172.17.0.2:27017/")
db = myclient["test"]
collection = db["test"]


