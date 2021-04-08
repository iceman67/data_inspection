from tasks import insert, maxmin
import json
import pandas as pd
import time

try:
    df = pd.read_csv('April_sensor_data.csv')
except OSError as err:
    print(f"OS error: {err}")
dic = {}
length = 20
tmp= df.head(length)

names = []
for i in tmp:
    names.append(i)
names = names[2:]

test_tmp = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
for i in names:
    dic[i] = list(test_tmp)

for i in range(length):
    for j in names:
        insert.delay(j, dic[j][i])
    time.sleep(1)

    for j in names:
        tmp = json.loads(maxmin.delay(j).get())
        print(f'sensor_name:{j} \t Large: {tmp[1]} / Small: {tmp[2]} / Avg: {tmp[3]}')
        #print(f'sensor_name:{j} \t {json.loads(maxmin.delay(j).get())[0]}')
    print('-----------------------')
    time.sleep(5)
