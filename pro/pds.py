import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# from statsmodels.tsa.stattools import adfuller
plt.style.use('seaborn-whitegrid')

df = pd.read_csv('April_sensor_data.csv').rename(columns={'PM10.0':'PM10', 'PM2.5':'PM25', 'Huminity':'Humidity'})
sensors = df[15840:25920]
sensors.fillna(method='pad')
names = sensors.columns[2:]

# 센서별 그래프 이미지 추출
def get_image(sensors, names):
    x = list(range(len(sensors['PM10'])))
    for i in names:
        fig = plt.figure(linewidth=0.1) #figsize=(20, 10)
        ax = fig.add_subplot(1, 1, 1)
        plt.title(f"{i}", fontsize = 18)
        plt.xlabel("Time", fontsize = 14)
        plt.ylabel("value", fontsize = 14)
        ax.set_xticks([0, 1440, 2880, 4320, 5760, 7200, 8640, 10080])
        ax.set_xticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'], fontsize = 10)
        ax.plot(x, sensors[i], color='blue')
        path = './static/img/'
        img_name = path + i + '.png'
        print(img_name)
        plt.savefig(img_name)
# get_image(sensors, names)

# 절대값 유사도 확인
def similarlity(sensors, names):
    interval = 60
    bins = (range(15840,25920,interval))
    bins_label = [str(x)+"이상 "+str(x+60)+"미만" for x in bins]
    sensors.loc[:,'level'] = pd.cut(sensors.loc[:,'NO'], bins, right=False, labels=bins_label[:-1])
        # sensors를 60개 단위(시간)로 cut + 구간별 평균 추출 -> 유사도 검사 시간단축
    res = sensors.groupby(['level']).mean()
    result = []
    for i in range(len(names)-1):
        for k in range(i,len(names)):
            if i == k:
                continue
            sum = 0
            for j in range(len(bins)-1):
                sum += abs(res[names[i]][j] - res[names[k]][j])
            result.append([int(sum),names[i],names[k]])
    result.sort(key=lambda x:x[0])
    score = []
    for i in range(len(result)):
        score.append([i,result[i][1],result[i][2]])
    return score
# smr = similarlity(sensors, names)
# for i in smr:
#     print(i)


# 상관관계 확인 -> corr
def pearson(sensors, names):
    cor = pd.DataFrame({i:sensors[i] for i in names}).corr()
    # print(cor) # 상관관계도 출력
    for i in names:
        cor[i] = np.where(cor[i] == 1, 0, cor[i])
    x = cor.index
    y = list(cor.idxmax(axis=1))
    dic = {}
    for i in range(len(y)):
        if cor[x[i]][y[i]] > 0.3:
            dic[x[i]] = y[i]
    return dic
# tRank = pearson(sensors, names)
