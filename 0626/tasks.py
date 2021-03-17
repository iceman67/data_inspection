from celery import Celery

HOST = '211.253.236.72'
PORT = 6379
DB=0

app = Celery('tasks', backend='redis://' + HOST+ ':' + str(PORT) +'/' + str(DB), broker='redis://' + HOST +':' + str(PORT) +'/' + str(DB))

@app.task
def insert(dname, value):
    pass

@app.task
def make_heap(doc):
    pass

@app.task
def maxmin(dname):
    pass

@app.task
def summary():
    pass

@app.task
def find(tf):
    pass

@app.task
def analy(dname, n):
    pass

@app.task
def analy_d(date):
    pass

@app.task
def delete():
    pass


if __name__ == '__main__':
    _main()


