#!/usr/bin/env python
#!coding:utf-8

import os
import time

class TPCHRunner:
    def __init__(self, tidbServer, queryPath, errPath, queryIdx):
        self.tidbServer = tidbServer
        self.queryPath  = queryPath
        self.errPath    = errPath
        self.queryIdx   = queryIdx

    def run(self):
        self.timeStart = time.time()
        sql = "{} < {}/{}.sql > /dev/null".format(self.tidbServer, self.queryPath, self.queryIdx)
        os.system(sql)
        self.timeStop  = time.time()
        self.timeRun = self.timeStop-self.timeStart

def run():
    queryPath  = "/Users/cs/code/goread/src/sql_script/sql"

    tidbServer = "mysql -h 127.0.0.1 -P4000 -u root -D test"

    resultFile = "/Users/cs/code/goread/src/sql_script/result"

    errPath = ""
    with open(resultFile, 'w') as f:
        for i in range(0, 1):
            if i == 15 :
                continue
            avgTime = 0
            num = 5
            for j in range(0, num):
                runnner = TPCHRunner(tidbServer, queryPath, errPath, i)
                runnner.run()
                print("query {} takes {}s".format(i, runnner.timeRun))
                f.write("query {} takes {}s\n".format(i, runnner.timeRun))
                f.flush()
                avgTime = avgTime + runnner.timeRun
            avgTime = avgTime / num
            print("query {} avgtimr {}s".format(i, avgTime))
            print("")


if __name__ == "__main__":
    run()
