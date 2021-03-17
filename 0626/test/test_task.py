from tasks import insert, sfind, analy, analy_d, delete
import os
import lm35_1
from datetime import datetime 
import json
import argparse

def parse_argument():
    usage = 'python test_task.py -f [device json file]\n       run with --help for argument descriptions'
    parser = argparse.ArgumentParser(usage = usage)
    parser.add_argument('-n', '--number', type=str, help='input analy count')
    parser.add_argument('-f', '--testfile', type=str, help='input sensor_json name')
    parser.add_argument('-c', '--command', type=str, help='input command')
    args=parser.parse_args()
    return args
args = parse_argument()

if(args.command == 'insert'):
    cnt = 0 
    while cnt < 2:
        date = int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19])
        dname = args.testfile
        data = lm35_1.sensing()
        doc = { "date":date, "dev_name":dname, "data":data }
        insert.delay(doc)
        cnt += 1
    print(f'insert data count : {cnt}')

if(args.command == 'sfind'):
    result = sfind.delay()
    res = json.loads(result.get())
    print(f'total doc count : {len(res)}')
    for i in res:
        print(i)

if(args.command == 'analy'):
    result = analy.delay(args.testfile, args.number)
    res = json.loads(result.get())
    print(f'최근 {args.number}개 평균:{res["avg"]},  최고:{res["max"]},  최저:{res["min"]}')

if(args.command == 'date'):
    date = int(args.number + (19 - len(args.number))*'0')
    result = analy_d(date)
    res = json.loads(result.get())
    for i in res:
        print(f'센서:{i["_id"]}, {date}이후,  평균:{i["avg"]},  최고:{i["max"]},  최저:{i["min"]}')
    

if(args.command == 'delete'):
    q = str(input('delete all? [Y/N]:'))
    if(q == 'Y' or q == 'y'):
        result = delete.delay()
        print('complete delete_all')
