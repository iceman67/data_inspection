from celery import Celery
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

conn = MongoClient('127.0.0.1:27017')
db = conn.sensor

rconn = redis.StrictRedis(
        host='127.0.0.1',
        port=6379,
        db=4)
rconn.set("cnt", 0)
rconn.set("heap", json.dumps([]))
rconn.set("lookup", json.dumps([]))
rconn.set("max_heap_length", 5)

@app.task
def insert(dname, value):
    doc = {"date": int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19]), "dev_name": dname, "data":value}
    tmp = db.device.find_one({ 'dev_name': doc['dev_name'] })
    max, min = int(tmp['valid_max']), int(tmp['valid_min'])
    TF = False
    if doc['data'] < min or doc['data'] > max:
        TF = True
    doc['error'] = TF
    app.send_task('tasks.make_heap', args=[json.dumps(doc)])
    db.data.insert(doc)

@app.task
def make_heap(doc):
    cnt, heap, lookup, max_heap_length = rconn.get("cnt"), json.loads(rconn.get("heap")), json.loads(rconn.get("lookup")), rconn.get("max_heap_length")
    print(f'cnt:{cnt}, max_heap_length={max_heap_length}, lookup={lookup}, heap={heap}')
    if cnt > max_heap_length-1:
        del heap[heap.index((lookup.pop(0), cnt-max_heap_length))]
    doc = json.loads(doc)
    lookup.append(doc['data'])
    heapq.heappush(heap, (doc['data'],cnt))
    cnt += 1
    maxmin = { "date":doc['date'], "dev_name":doc['dev_name'], "max":heapq.nlargest(1,heap)[0][0], "min":heapq.nsmallest(1,heap)[0][0] }
    rconn.set("cnt", cnt)
    rconn.set("heap", json.dumps(heap))
    rconn.set("lookup", json.dumps(lookup))
    db.maxmin.insert(maxmin)
 

@app.task
def maxmin():
    cnt = rconn.get("cnt")
    heap = json.loads(rconn.get("heap"))
    return cnt, heap
    '''
    print(heapq.nlargest(1,heap))
    return (heapq.nlargest(1,heap)[0][0] ,heapq.nsmallest(1,heap)[0][0])
    '''




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
