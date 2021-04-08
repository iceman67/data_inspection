from tasks import insert
import lm35_1
import heapq
from datetime import datetime
import time

heap = []
lookup = []
length_max = 5
cnt = 0

def make_doc(dname, data):
    doc = {"date": int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19]), "dev_name": dname, "data": data}
    return doc

while True:
    time.sleep(1)
    if cnt > length_max-1:
        del heap[heap.index((lookup.pop(0),cnt-length_max))]
    data = lm35_1.sensing()
    lookup.append(data)
    heapq.heappush(heap, (data, cnt))
    insert.delay(make_doc('lm35_1', data))
    cnt += 1
    print(f'max ={heapq.nlargest(1,heap)[0][0]}, min ={heapq.nsmallest(1,heap)[0][0]}')
    print(heap)
    print(lookup)
    print('--------------')

while True:
    time.sleep(1)
    if cnt > length_max-1:
        del heap[heap.index((lookup.pop(0),cnt-length_max))]
    data = lm35_1.sensing()
    lookup.append(data)
    heapq.heappush(heap, (data, cnt))
    insert.delay(make_doc('lm35_1', data))
    cnt += 1
    print(f'max ={heapq.nlargest(1,heap)[0][0]}, min ={heapq.nsmallest(1,heap)[0][0]}')
    print('--------------')
