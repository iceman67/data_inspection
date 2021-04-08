import redis
import json
'''
con = redis.StrictRedis(
        host='211.253.236.72',
        port=6379,
        db=0)

POOL = redis.ConnectionPool(host='211.253.256.72', port=6379, db=0)
try:
    rconn = redis.Redis(connection_pool=POOL)
    rconn.ping()
    print('redis connection')
except redis.ConnectionError:
    print('connection fail')
'''

HOST = '211.253.236.72'
PORT = 6379
DB=0

rconn = None
init = None
POOL = redis.ConnectionPool(host=HOST, port=PORT, db=DB)
try:
    rconn = redis.Redis(connection_pool=POOL)
    rconn.ping()
    print ("Redis is connected!")
except redis.ConnectionError:
   print ("Redis connection error!")
init = {'cnt':0, 'heap':[], 'lookup':[], 'max_length':5}





names = ['PM2.5', 'PM10.0', 'Temperature', 'Humidity', 'TVOC', 'CO2']
for i in names:
    rconn.delete(i)
