import redis
import json
try:
    conn = redis.StrictRedis(
        host='127.0.0.1',
        port=6379,
        db=2)
    '''
    print ('Set Record:', conn.set("test", "Nice to meet you"))
    print ('Get Record:', conn.get("test"))
    print ('Delete Record:', conn.delete("test"))
    print ('Get Deleted Record:', conn.get("test"))
except Exception as ex:
    print ('Error:', ex)
    '''
    a = 'lm35_1'
    conn.set(a, json.dumps({'cnt':1, 'heap':[12], 'lookup':[123], 'length':5}))
    tmp = json.loads(conn.get(a))
    print(type(tmp['cnt']))
    print(type(tmp['heap']))
    print(type(tmp['length']))
except Exception as ex:
    print('Error:', ex)
