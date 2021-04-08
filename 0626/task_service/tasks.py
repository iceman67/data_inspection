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

#HOST = '211.253.236.72'
HOST = '127.0.0.1'
PORT = 6379
DB=0

#app = Celery('tasks', backend='rpc://',  broker='pyamqp://')
#app = Celery('tasks', backend='redis://localhost:6379/0',  broker='redis://211.253.236.74:6379/0')
#app = Celery('tasks', backend='redis://'+HOST+ ':6379/0',  broker='redis://' + HOST +':6379/0')
app = Celery('tasks', backend='redis://' + HOST+ ':' + str(PORT) +'/' + str(DB), broker='redis://' + HOST +':' + str(PORT) +'/' + str(DB))
#app = Celery('tasks', backend='rpc://',  broker='redis://211.253.236.74:6379/0')

'''
#rconn = redis.Redis(
rconn = redis.StrictRedis(
        #host='127.0.0.1',
        host='211.253,236.74',
        port=6379,
        db=0)
'''

'''
POOL = redis.ConnectionPool(host=HOST, port=PORT, db=DB)
try:
    rconn = redis.Redis(connection_pool=POOL)
    rconn.ping()
    print ("Redis is connected!")
except redis.ConnectionError:
    print ("Redis connection error!")
init = {'cnt':0, 'heap':[], 'lookup':[], 'max_length':10}
'''

rconn = None
init = None
POOL = None
def _main():
    POOL = redis.ConnectionPool(host=HOST, port=PORT, db=DB)
    try:
        rconn = redis.Redis(connection_pool=POOL)
        rconn.ping()
        print ("Redis is connected!")
    except redis.ConnectionError:
       print ("Redis connection error!")
    init = {'cnt':0, 'heap':[], 'lookup':[], 'max_length':5}


@app.task
def insert(dname, value):
    doc = {"date": int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19]), "dev_name": dname, "data":value}
    app.send_task('tasks.make_heap', args=[json.dumps(doc)])

@app.task
def make_heap(doc):
    doc = json.loads(doc)
    rconn = redis.Redis(connection_pool=POOL)
    data = json.loads(rconn.get(doc["dev_name"]))
    if data == None:
        init = {'cnt':0, 'heap':[], 'lookup':[], 'max_length':5}
        rconn.set(doc["dev_name"], json.dumps(init))
        data = json.loads(rconn.get(doc["dev_name"]))
    cnt, max_length, lookup, heap = data["cnt"], data["max_length"], data["lookup"], data["heap"]
    print(f'dev_name:{doc["dev_name"]}, cnt:{cnt}, max_length={max_length}, lookup={lookup}, heap={heap}')
    
    if cnt > max_length-1:
        del heap[heap.index([lookup.pop(0), cnt-max_length])]
    lookup.append(doc['data'])
    heapq.heappush(heap, [doc['data'],cnt])
    cnt += 1
    data["cnt"], data["max_length"], data["lookup"], data["heap"] = cnt, max_length, lookup, heap
    rconn.set(doc["dev_name"], json.dumps(data))
 

@app.task
def maxmin(dname):
    rconn = redis.Redis(connection_pool=POOL)
    tmp = json.loads(rconn.get(dname))
    heap = tmp["heap"]
    avg = sum(tmp["lookup"])/len(tmp["lookup"])
    tmp = [heap, heapq.nlargest(1,heap)[0][0], heapq.nsmallest(1,heap)[0][0], avg]
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


if __name__ == '__main__':
    _main()

