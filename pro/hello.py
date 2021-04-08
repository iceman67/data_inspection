#hello.py
from flask import Flask, render_template, flash, redirect, request, session, abort, make_response
import pds
import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt
from functools import wraps, update_wrapper
from datetime import datetime

df = pd.read_csv('April_sensor_data.csv').rename(columns={'PM10.0':'PM10', 'PM2.5':'PM25', 'Huminity':'Humidity'})
sensors = df[15840:25920]
sensors.fillna(method='pad')
names = sensors.columns[2:]

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
@app.after_request
def set_response_headers(r):
    r.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    r.headers['Pragma'] = 'no-cache'
    r.headers['Expires'] = '0'
    return r

@app.route('/')
def hello():
    img_list = map(lambda x:'img/' + x + '.png', list(names))
    return render_template('img_static.html', filename=img_list)
    # return render_template('img_static.html', filename='img/PM10.png')

@app.route('/reload')
def reload():
    print('reload')
    pds.get_image(sensors, names)
    time.sleep(1)
    return hello()

if __name__ == '__main__':
    app.run()
