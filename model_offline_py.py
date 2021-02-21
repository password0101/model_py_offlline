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