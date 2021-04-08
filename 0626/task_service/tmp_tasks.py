from celery import Celery
import pandas as pd
import redis
import json
import pymongo
import time
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson import json_util
import heapq


app = Celery('tasks', backend='rpc://',  broker='pyamqp://')

rconn = redis.StrictRedis(
        host='127.0.0.1',
        port=6379,
        db=2)
device_name = "lm35_1"
init = {'cnt':0, 'heap':[], 'lookup':[], 'max_length':10}
rconn.set(device_name, json.dumps(init))

@app.task
def insert(dname, value):
    doc = {"date": int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19]), "dev_name": dname, "data":value}
    app.send_task('tasks.make_heap', args=[json.dumps(doc)])

@app.task
def make_heap(doc):
    doc = json.loads(doc)
    data = json.loads(rconn.get(doc["dev_name"]))
    cnt, max_length, lookup, heap = data["cnt"], data["max_length"], data["lookup"], data["heap"]
    print(f'cnt:{cnt}, max_length={max_length}, lookup={lookup}, heap={heap}')
    
    if cnt > max_length-1:
        del heap[heap.index([lookup.pop(0), cnt-max_length])]
    lookup.append(doc['data'])
    heapq.heappush(heap, [doc['data'],cnt])
    cnt += 1
    print(heap)
    data["cnt"], data["max_length"], data["lookup"], data["heap"] = cnt, max_length, lookup, heap
    rconn.set(doc["dev_name"], json.dumps(data))
 

@app.task
def maxmin():
    heap = json.loads(rconn.get("lm35_1"))["heap"]
    tmp = [heap, heapq.nlargest(1,heap)[0][0], heapq.nsmallest(1,heap)[0][0]]
    #
    return json.dumps(tmp)




@app.task
def find(tf):
    if tf == None:
        doc_list = list(db.data.find( {},{'_id':0}))
    else:
        doc_list = list(db.data.find({ 'error':tf },{ '_id':0 }))
    return str(json.dumps(doc_list, default=json_util.default))

@app.task
def analy(dname, n):
    Pipeline = list()
    Pipeline.append({'$match' : {'dev_name':dname, 'error':False}})
    Pipeline.append({'$sort' : {'date':-1}})
    Pipeline.append({'$limit' : int(n)})
    Pipeline.append({'$group' : {'_id' : 'data_analy', 'avg' : {'$avg':'$data'}, 'max' : {'$max':'$data'}, 'min' : {'$min':'$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))

@app.task
def analy_d(date):
    Pipeline = list()
    Pipeline.append({'$match' : {'date':{'$gte':date}}})
    Pipeline.append({'$group' : {'_id' : '$dev_name', 'avg' : {'$avg':'$data'}, 'max' : {'$max':'$data'}, 'min' : {'$min':'$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))

@app.task
def delete():
    db.data.drop()
