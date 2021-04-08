from tasks import insert, maxmin
import json
import pandas as pd
import time
re_count = 0
max_cnt = 20

try:
    df = pd.read_csv('April_sensor_data.csv')
except OSError as err:
    print(f"OS error: {err}")
dic = {}
length = 100
tmp= df.head(length)

print (type(tmp))
# get column name 
names = []
for i in tmp:
    names.append(i)
names = names[2:]

print (f'names = {names}')

for i in names:
    dic[i] = list(tmp[i])
    print (f'{i} = {dic[i]}')


####

import redis

#HOST = '211.253.236.72'
HOST = '127.0.0.1'
PORT = 6379
DB=0
rconn = None
init = None
POOL = None
POOL = redis.ConnectionPool(host=HOST, port=PORT, db=DB)
try:
    rconn = redis.Redis(connection_pool=POOL)
    rconn.ping()
    print ("Redis is connected!")
except redis.ConnectionError:
   print ("Redis connection error!")

rconn = redis.Redis(connection_pool=POOL)
init = {"cnt":0, "heap":[], "lookup":[], "max_length":20}
for dname in names:
    rconn.set(dname, json.dumps(init))

for dname in names:
    tmp = rconn.get(dname)
    print(dname, tmp)
####

for i in range(length):
    re_count+=1
    print(re_count)
    if re_count % max_cnt == 0:
        time.sleep(5)
        continue

    for j in names:
        insert.delay(j, dic[j][i])

    for j in names:
        tmp = json.loads(maxmin.delay(j).get())
        print (f'tmp = {tmp}')
        print(f'sensor_name:{j} \t Large: {tmp[1]} / Small: {tmp[2]} / Avg: {tmp[3]}')
    print('-----------------------')
    time.sleep(0.5)

