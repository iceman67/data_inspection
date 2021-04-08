from tasks import insert, maxmin
import lm35_1
import time
import json
while True:
    
    insert.delay('lm35_1', lm35_1.sensing())
    time.sleep(1)
    
    result = maxmin.delay('lm35_1')
    tmp = json.loads(result.get())
    #print(tmp)
    print(f'Heap : {tmp[0]}')
    print(f'Large :{tmp[1]} / Small : {tmp[2]}')
    print('-------------------------')
    time.sleep(1)
