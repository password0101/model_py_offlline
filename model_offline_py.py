#!/usr/bin/python

from __future__ import print_function
import datetime
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%s')
    print('\n')
    print("begin as ---------->", nowTime)
    print('\n')

    parameters = sys.argv[1]
    print("paramsters:", parameters)
    print('\n')

    appName = ''
    service = 'default'
    algorithm = 'default'
    parameterAlg = []
    parameterList = parameters.strip().split()

    for i in range(len(parameterList)):
        parameter = parameterList[i]

        if i == 0:
            appName = parameter
        elif i == 1:
            service = parameter
        elif i == 2:
            algorithm = parameter
        else:
            parameterAlg.append(parameter)

    appName = 'modelOfflinePySpark' + '_' + appName
    spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()

    