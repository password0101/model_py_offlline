#!/usr/bin/python

from __future__ import print_function
import datetime
import sys
from pyspark.sql import SparkSession
import math
import numpy as np
import xgboost as xgb

from sklearn import metrics
from sklearn.metrics import  confusion_matrix, accuracy_score
from scipy.stats import ks_2samp
from pyspark.sql.types import *
from pyarrow import orc as orc
import os
import datetime

class XgbModel(object):

    def __init__(self, algorithm, parameters, executor):
        self.algorithm = algorithm
        self.parameters = parameters
        self.executor = executor

    def manipulate(self):

        if self.algorithm == 'xgbTrain':
            self.xgbTrain()
        elif self.algorithm == 'xgbPredict':
            self.xgbPredict()

        else:
            print("No support parameters: ", self.algorithm)

    def xgbTrain(self):
        print('xgbTrain')

        inputTrainTable = ''
        label = ''
        features = ''
        inputTestTable = ''
        outputModelDir = ''

        threshold = 0.5

        num_round = 100

        objective = 'binary:logistic'
        eval_metric = 'auc'
        verbosity = 1
        eta = 0.3
        gamma = 0
        max_depth = 6
        min_child_weight = 1
        max_delta_step = 0
        subsample = 1.0
        colsample_bytree = 1.0
        colsample_bylevel = 1.0
        colsample_bynode = 1.0
        reg_lambda = 1.0
        reg_alpha = 0.0
        tree_method = 'auto'
        sketch_eps = 0.03
        scale_pos_weight = 1
        refresh_leaf = 1
        process_type = 'default'
        grow_policy = 'depthwise'
        max_leaves = 0
        max_bin = 256
        predictor = 'cpu_predictor'
        num_parallel_tree = 1
        seed = 0
        base_score = 0.5

        for pair in self.parameters:
            print("modify parameters:", pair)
            kv = pair.split(':')
            if kv[0] == 'inputTrainTable':
                inputTrainTable = kv[1]
            elif kv[0] == 'inputTestTable':
                inputTestTable = kv[1]
            elif kv[0] == 'outputModelDir':
                outputModelDir = kv[1]
            elif kv[0] == 'label':
                label = kv[1]
            elif kv[0] == 'features':
                features = kv[1]
            elif kv[0] == 'objective':
                objective = int(kv[1])
            elif kv[0] == 'eval_metric':
                eval_metric = kv[1]
            elif kv[0] == 'num_round':
                num_round = int(kv[1])
            elif kv[0] == 'threshold':
                threshold = float(kv[1])
            elif kv[0] == 'verbosity':
                verbosity = int(kv[1])
            elif kv[0] == 'nthread':
                nthread = int(kv[1])
            elif kv[0] == 'eta':
                eta = float(kv[1])
            elif kv[0] == 'gamma':
                gamma = int(kv[1])
            elif kv[0] == 'max_depth':
                max_depth = int(kv[1])
            elif kv[0] == 'min_child_weight':
                min_child_weight = int(kv[1])
            elif kv[0] == 'max_delta_step':
                max_delta_step = int(kv[1])
            elif kv[0] == 'subsample':
                subsample = float(kv[1])
            elif kv[0] == 'colsample_bytree':
                colsample_bytree = float(kv[1])
            elif kv[0] == 'colsample_bylevel':
                colsample_bylevel = float(kv[1])
            elif kv[0] == 'colsample_bynode':
                colsample_bynode = float(kv[1])
            elif kv[0] == 'reg_lambda':
                reg_lambda = float(kv[1])
            elif kv[0] == 'reg_alpha':
                reg_alpha = float(kv[1])
            elif kv[0] == 'tree_method':
                tree_method = kv[1]
            elif kv[0] == 'sketch_eps':
                sketch_eps = float(kv[1])
            elif kv[0] == 'scale_pos_weight':
                scale_pos_weight = int(kv[1])
            elif kv[0] == 'updater':
                updater = kv[1]
            elif kv[0] == 'refresh_leaf ':
                refresh_leaf = int(kv[1])
            elif kv[0] == 'process_type':
                process_type = kv[1]
            elif kv[0] == 'grow_policy':
                grow_policy = kv[1]
            elif kv[0] == 'max_bin':
                max_bin = int(kv[1])
            elif kv[0] == 'predictor':
                predictor = kv[1]
            elif kv[0] == 'num_parallel_tree':
                num_parallel_tree = int(kv[1])
            elif kv[0] == 'seed':
                seed = int(kv[1])
            elif kv[0] == 'base_score':
                base_score = int(kv[1])
            else:
                print("No support parameter:", kv[0])

        column = features + ',' + label

        trainDF = self.executor.fetchData(column = column, table = inputTrainTable)
        trainDF.show(1)
        trainDataTmp = trainDF.toPandas()
        trainX = trainDataTmp[features.strip().split(',')].values
        trainY = trainDataTmp[label.strip().split(',')].values
        trainDM = xgb.DMatrix(trainX, label= trainY, missing=-999.0)

        testDF = self.executor.fetchData(column=column, table= inputTestTable)
        testDF.show(1)
        testDataTmp = testDF.toPandas()
        testX = testDataTmp[features.strip().split(',')].values
        testY = testDataTmp[label.strip().split(',')].values
        testDM = xgb.DMatrix(testX, label= testY, missing=-999.0)

        evallist = [(testDM, 'eval'), (trainDM, 'train')]

        param = {
            'objective': objective,
            'eval_metric': eval_metric,
            'verbosity': verbosity,
            'eta': eta,
            'gamma': gamma,
            'max_depth': max_depth,
            'min_child_weight': min_child_weight,
            'max_delta_step': max_delta_step,
            'subsample': subsample,
            'colsample_bytree': colsample_bytree,
            'colsample_bylevel': colsample_bylevel,
            'colsample_bynode': colsample_bynode,
            'reg_lambda': reg_lambda,
            'reg_alpha': reg_alpha,
            'tree_method': tree_method,
            'sketch_eps': sketch_eps,
            'scale_pos_weight': scale_pos_weight,
            'refresh_leaf': refresh_leaf,
            'process_type': process_type,
            'grow_policy': grow_policy,
            'max_leaves': max_leaves,
            'max_bin': max_bin,
            'predictor': predictor,
            'num_parallel_tree': num_parallel_tree,
            'seed': seed,
            'base_score': base_score
        }
        bst = xgb.train(param, trainDM, num_round, evallist)

        testProbPred = bst.predict(testDM)
        testYPred = [1 if x >= threshold else 0 for x in testProbPred]
        self.resultCheck(testY,testYPred, testProbPred)

        bst.save_model(outputModelDir)







    def xgbPredict(self):
        print("xgbPredict")

        dirName = ''
        outputPredTable = ''
        outputPredName = ''
        inputModelDir = ''
        features = ''
        reserveFields = ''

        for pair in self.parameters:
            print("Modify pair", pair)
            kv = pair.spilt(':')

            if kv[0] == 'dirName':
                dirName = kv[1]
            elif kv[0] == 'outputPredTable':
                outputPredTable = kv[1]
            elif kv[0] == 'outputPredName':
                outputPredName = kv[1]
            elif kv[0] == 'inputModelDir':
                inputModelDir = kv[1]
            elif kv[0] == 'features':
                features = kv[1]
            elif kv[0] == 'reserveFields':
                reserveFields = kv[1]
            else:
                print("No support parameter:", kv[0])

        path = '/home/path' + dirName
        print("load model--------------")
        bst = xgb.Booster(model_file=inputModelDir)
        files = os.listdir(path)
        reserveCnt = len(reserveFields.spilt(','))
        featureCnt = len(features.spilt(','))
        totalCnt = reserveCnt + featureCnt
        insertTimes = 0

        for file in files:
            print(f"dealing with file: {file}")
            with open(os.path.join(path, file), 'rb') as f:
                predDataTmp = orc.ORCFile(f).read().to_pandas()
                print(predDataTmp.shape)
                predX = predDataTmp.iloc[:, reserveCnt:totalCnt].values
                print("begin -------------")
                predDM = xgb.DMatrix(predX, missing=-999.0)
                preds = bst.predict(predDM)
                print("deal--------------")
                fields = predDataTmp.iloc[:, 0: reserveCnt].astype('str').values

                filedsNP = np.array(fields).T
                predsNP = np.array(preds)

                reserveFiledsConcatPreds = np.vstack((filedsNP, predsNP)).T.tolist()

                fieldNamesStr = reserveFields + ',' + outputPredName
                fieldNamesList = fieldNamesStr.spilt(',')

                fieldsStructed = [StructField(fieldName, StringType(), True) for fieldName in fieldNamesList]
                schema = StructType(fieldsStructed)

                print("save---------------")
                self.executor.saveDataSample(reserveFiledsConcatPreds, schema, outputPredTable, insertTimes)

            insertTimes = insertTimes+1
            nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%s')

            print('\n')
            print("now as ---------->", nowTime)
            print('\n')

    def resultCheck(self, labels, preds, probs):
        print("check")











