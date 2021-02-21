#!/usr/bin/python

from __future__ import print_function


class ExecHive(object):

    def __init__(self, spark):
        self.spark = spark

    def fetchData(self, sql):
        return self.spark.sql(sql)

    def fetchData(self, column, table):
        sql = 'select ' + column + ' from ' + table
        return self.spark.sql(sql)

    def fetchDataSample(self, column, table, key, value, batch):
        sql = ''
        if batch == 10:
            sql = 'select ' + column + ' from ' + table + ' where sunstring(' + key + ',-1)' + str(value)
        elif batch == 100:
            sql = 'select ' + column + ' from ' + table + ' where sunstring(' + key + ',-2)' + str(value)
        elif batch == 1000:
            sql = 'select ' + column + ' from ' + table + ' where sunstring(' + key + ',-3)' + str(value)
        else:
            print('Illegal!!')

        return self.spark.sql(sql)

    def saveData(self, data, schems, output):
        self.spark.createDataFrame(data, schems, samplingRatio=None).createOrReplaceTempView("tempTable")
        sql = 'insert overwrite table' + ' ' + output + ' ' + 'select *, now() as etl_time from' + ' ' + 'tempTable'
        self.spark.sql(sql)


    def saveDataSample(self, data, schems, output, value):
        self.spark.createDataFrame(data, schems, samplingRatio=None).createOrReplaceTempView("tempTable")

        if value == 0:
            sql = 'insert overwrite table' + ' ' + output + ' ' + 'select *, now() as etl_time from' + ' ' + 'tempTable'
            self.spark.sql(sql)
        else:
            sql = 'insert into table' + ' ' + output + ' ' + 'select *, now() as etl_time from' + ' ' + 'tempTable'
            self.spark.sql(sql)


    def saveDF(self, data, output):
        self.spark.createDataFrame(data).createOrReplaceTempView("tempTable")
        sql = 'insert overwrite table' + ' ' + output + ' ' + 'select *, now() as etl_time from' + ' ' + 'tempTable'
        self.spark.sql(sql)