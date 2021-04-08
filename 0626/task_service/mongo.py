import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson import json_util

conn = MongoClient('127.0.0.1:27017')
print(conn)
print('log1')

db = conn.sensor
print(db)
print('log2')

db.data.insert({'ff':'aa'})
print('log3')
