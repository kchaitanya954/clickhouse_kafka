import numpy as np
import sys
import time
from clickhouse_driver import Client, connect
import datetime
import json

def genDict(): 
    randList = np.random.dirichlet(np.ones(5),size=1)[0]
    maxList = [0]*5
    maxInd = np.argmin(randList)
    maxList[maxInd] = 1
    today = datetime.datetime.now()
    time = today.strftime("%Y-%m-%d %H:%M:%S")
    
    maxList.insert(0, str(time))
    
    events = ["time", "sports", "crime", "covid", "Business", "Political"]
    eventDict = dict(zip(events, maxList))
    # print(eventDict)
    # time.sleep(1)
    return json.dumps(eventDict)

client = Client('localhost')
client.execute('use trail')
client.execute('truncate table test')
# client.execute('create database trail')
# print(client.execute('SHOW databases'))

print(genDict())

client.execute("create table IF NOT EXISTS test(time DateTime('Europe/Moscow'), sports Float32,crime Float32,\
    covid Float32,Business Float32,Political Float32) ENGINE = MergeTree() ORDER BY time")

try:
    while True:
        client.execute("INSERT INTO test format JSONEachRow {}".format(genDict()))
        time.sleep(1)
except KeyboardInterrupt:
    sys.exit()
