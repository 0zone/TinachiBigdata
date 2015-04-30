# encoding: utf-8
# !/usr/bin/python

from util import *
import MySQLdb
import os
import math
import datetime
import random
import time
from sklearn import preprocessing
from sklearn.externals import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn import linear_model
from sklearn import cross_validation
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import make_scorer
from sklearn import datasets
from sklearn.metrics import f1_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.cross_validation import train_test_split
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import classification_report
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.svm import SVC
from sklearn import tree
from sklearn import svm
import numpy as np

#数据库配置
sqlhost = '127.0.0.1'
sqluser = 'root'
sqlpswd = 'root'
sqldb = 'tianchi'


def endtime(starttime, desc, issecond=True):
    endtime = datetime.datetime.now()
    if issecond:
        interval = (endtime - starttime).seconds
        print desc + ' cost:' + str(interval) + ' sec'
    else:
        interval = (endtime - starttime).microseconds
        print desc + ' cost:' + str(interval) + 'microsec'
    return datetime.datetime.now()


#对样本进行划分（正负样本）
def building(file4feat, file4label):
    starttime = datetime.datetime.now()
    #获取特征和标签
    uipairs, feats = load_matrix(file4feat)  #{'user_id,item_id':[...]}
    lastdayposnum, labels = load_label(file4label, uipairs)  #{'user_id,item_id':0}

    # posenum = 0#正样本
    # neganum = 0#负样本
    # featlist = []
    # labellist = []
    # uipair = []
    # for row in feats:
    # 	row_label = int(labels.get(row, 0))
    # 	if row_label == 1:
    # 		posenum += 1
    # 	else:
    # 		neganum += 1
    #
    # 	labellist.append(row_label)
    # 	uipair.append(row)
    # 	featlist.append(feats[row])


    # #对特征进行归一化处理
    # #feat_scaled = preprocessing.scale(featlist)
    # #feat_scaled = preprocessing.normalize(featlist)#
    # feat_scaled = featlist
    # starttime = endtime(starttime,'build samples')
    #
    # percent = (float)(posenum)/neganum
    # print 'num of pos samples:'+str(posenum)+" ,num of neg examples:"+str(neganum)+",percent of both:"+str(percent)

    # return uipair,feat_scaled,labellist
    feats = preprocessing.scale(feats)
    return uipairs, feats, labels, lastdayposnum


def slip_windows(windows_list):
    starttime = datetime.datetime.now()
    uipairs = np.array([])
    feats = []
    labels = np.array([])
    for window in windows_list:
        cur_uipairs, cur_feats = load_matrix(window['feat'])  #{'user_id,item_id':[...]}
        cur_lastdayposnum, cur_labels = load_label(window['label'], cur_uipairs)  #{'user_id,item_id':0}
        if len(feats) == 0:
            feats = cur_feats
        else:
            feats = np.concatenate((feats, cur_feats))
        labels = np.concatenate((labels, cur_labels))
        uipairs = np.concatenate((uipairs, cur_uipairs))

    posnum = len([a for a in labels if a == 1])  #正样本
    negnum = len(labels) - posnum
    print 'after slip_windows clean,sample posenum is ' + str(posnum) + ' and negnum is ' + str(negnum)
    endtime(starttime, 'slip_windows')

    return uipairs, feats, labels, cur_lastdayposnum


#下抽样
def sample4train(uipair, feat, label, perc):  #perc:负样本：正样本 = 多少比1
    #抽样
    feat_sample = []  #[[],[],...]
    label_sample = []  #[1,1,...]
    uipair_sample = []  #[uid_iid,uid_iid,...]

    posenum = 0
    neganum = 0
    negaindex = []
    index = 0
    for la in label:
        if la == 1:
            posenum += 1
            label_sample.append(1)
            uipair_sample.append(uipair[index])
            feat_sample.append(feat[index])
        else:
            neganum += 1
            negaindex.append(index)
        index += 1

    sample_neganum = perc * posenum
    for count in range(0, sample_neganum):
        neg = random.randint(0, neganum - 1)
        index = negaindex[neg]

        label_sample.append(0)
        uipair_sample.append(uipair[index])
        feat_sample.append(feat[index])

    return uipair_sample, feat_sample, label_sample


#对样本进行划分（正负样本）
def prepare4train(file4feat, file4label):
    uipair, feat, label, lastdayposnum = building(file4feat, file4label)

    #uipair,feat,label = rule2(uipair,'2014-12-14','2014-12-16',feat,label)
    #rulepool4train(uipair,lastday,feat,label)
    #抽样
    uipair, feat, label = sample4train(uipair, feat, label, 130)
    feat, label = createsamples(feat, label, 25000)

    posnum = len([a for a in label if a == 1])
    negnum = len(label) - posnum
    print 'num of pos samples:' + str(posnum) + ' num of neg samples:' + str(negnum)

    return uipair, feat, label, lastdayposnum


#对样本进行划分（正负样本）
def prepare4offlinepredict(file4feat, file4label):
    uipair, feat, label, lastdayposnum = building(file4feat, file4label)
    return uipair, feat, label, lastdayposnum


#对样本进行划分（正负样本）
def prepare4onlinepredict(file4feat):
    # #获取特征
    # feats = load_matrix(file4feat)#{'user_id,item_id':[...]}
    # uipair = []
    # featlist = []
    #
    # for row in feats:
    # 	uipair.append(row)#uid,iid
    # 	featlist.append(feats[row])#存放特征的队列
    #
    # #对特征进行归一化处理
    # #feat_scaled = preprocessing.scale(featlist)
    # #feat_scaled = preprocessing.normalize(featlist)#
    # feat_scaled = featlist
    #
    # return uipair,feat_scaled

    uipairs, feats = load_matrix(file4feat)
    #feats = preprocessing.normalize(feats)
    return uipairs, feats


def createsamples(feats, labels, counts):
    starttime = datetime.datetime.now()

    clf = ExtraTreesClassifier()
    clf.fit(feats, labels)
    feat_scores = clf.feature_importances_  #特征重要性

    clf = linear_model.LogisticRegression()
    clf.fit(feats, labels)
    param = clf.coef_  #特征正负相关性

    feats_mean = np.mean(feats, axis=0)
    feats_std = np.std(feats, axis=0)
    feat_scores_sorted = sorted(feat_scores, reverse=True)

    feats_new_mean = []
    for i in range(0, len(feat_scores)):
        feat_rank = feat_scores_sorted.index(feat_scores[i])
        std_delta = float(len(feat_scores) - feat_rank) / len(feat_scores) * feats_std[i]
        if param[0][i] > 0:
            feats_new_mean.append(feats_mean[i] + std_delta)
        else:
            feats_new_mean.append(feats_mean[i] - std_delta)

    featvalue = np.array([])
    for i in range(0, len(feat_scores)):
        current_featvalue = np.random.normal(feats_new_mean[i], float(feats_std[i]) / 2, counts)
        featvalue = np.append(featvalue, current_featvalue)

    featvalue = featvalue.reshape(len(feat_scores), -1).T
    new_labels = np.ones(counts)

    feats = np.concatenate((feats, featvalue))
    labels = np.append(labels, new_labels)
    endtime(starttime, 'createsamples')

    return feats, labels


#训练分类器
def train(file4feat, file4label, lastday):
    #1.建立测试集 抽取特征和标记

    uipair, feat, label, lastdayposnum = prepare4train(file4feat, file4label)


    #2.CV选取超参数
    #2.1设置分类器
    estimators = {}
    estimators['LR'] = linear_model.LogisticRegression(class_weight={1: 25, 0: 1})  #class_weight = {1:1.5,0:1}
    #estimators['RF'] = RandomForestClassifier()
    #estimators['GBDT'] = GradientBoostingClassifier()
    #estimators['bayes'] = GaussianNB()
    #estimators['tree'] = tree.DecisionTreeClassifier()
    #estimators['SVM'] = svm.SVC()

    #2.2设置超参数
    tuned_parameters = {}
    tuned_parameters['LR'] = [{'C': [1,0.1,0.001],'penalty':['l2','l1'],'class_weight':[{1:25,0:1}, 'auto']}]
    #tuned_parameters['RF'] = [{'n_estimators': [10,100], 'max_depth': [1,3,5,7,9]}]
    #tuned_parameters['GBDT'] = [{'n_estimators': [10,100], 'max_depth': [1,3,5,7,9]}]
    #tuned_parameters['bayes'] = [{'alpha': [1,0,10]}]
    #tuned_parameters['tree'] = [{'max_depth':[1,10,100,1000,None]}]
    #tuned_parameters['SVM'] = [{'kernel': ['rbf'], 'gamma': [1e-3, 1e-4],'C': [1, 10, 100, 1000]},{'kernel': ['linear'], 'C': [1, 10, 100, 1000]}]

    #2.3训练超参数
    starttime = datetime.datetime.now()
    for k in estimators.keys():
        print("# Tuning hyper-parameters for %s" % k)
        #estimators[k] = GridSearchCV(estimators[k], tuned_parameters[k], cv=5, scoring="f1")  #不做交叉验证

        estimators[k].fit(feat, label)

        # for params, mean_score, scores in estimators[k].grid_scores_:
        # print("%0.3f (+/-%0.03f) for %r" % (mean_score, scores.std() / 2, params))

        label_pred = estimators[k].predict(feat)

        starttime = endtime(starttime, k + ' train')

    return estimators


def saveres(filename, uipair, label):
    #3.2记录结果
    recPairs = ''
    posenum = 0
    sum = 0
    for p in label:
        if p == 1:
            recPairs += str(int(uipair[sum][0])) + ',' + str(int(uipair[sum][1])) + '\n'  #uid,iid
            posenum += 1
        sum += 1

    neganum = sum - posenum
    percent = (float)(posenum) / neganum
    print 'predict num of pos samples:' + str(posenum) + " ,num of neg examples:" + str(
        neganum) + ",percent of both:" + str(percent)

    fout = open(filename, "w")
    fout.write("user_id,item_id\n" + recPairs)
    fout.close()


#规则1 昨天是否加入购物车
def rule1(uipairs, lastday, feat=[], label=[]):
    conn = MySQLdb.connect(host=sqlhost, user=sqluser, passwd=sqlpswd, db=sqldb, charset='utf8', port=3306)
    cur = conn.cursor()
    sql = "select DISTINCT user_id,item_id from train_user_target_item_new where behavior_type = 3 and date(time) = '" + lastday + "'"
    cur.execute(sql)
    results = cur.fetchall()
    ui_list = uipairs.tolist()

    if len(label) != 0:
        new_uipairs = np.array([])
        new_feat = np.array([])
        new_label = np.array([])
        for row in results:
            uipair = str(row[0]) + ',' + str(row[1])
            if uipair in ui_list:
                i = ui_list.index(uipair)
                new_feat.add(feat[i])
                new_uipairs.add(uipairs[i])
                new_label.add(label[i])

    posnum = len([a for a in new_label if a == 1])  #正样本
    negnum = len(new_label) - posnum
    print 'after rule1 clean,sample posenum is ' + str(posnum) + ' and negnum is ' + str(negnum)

    return new_uipairs, new_feat, new_feat


#最近收藏夹的ui对
def rule2(uipairs, beginday, endday, feat, label):
    conn = MySQLdb.connect(host=sqlhost, user=sqluser, passwd=sqlpswd, db=sqldb, charset='utf8', port=3306)
    cur = conn.cursor()
    sql = "select DISTINCT user_id,item_id from train_user_target_item_new where behavior_type = 3 and date(time) >= '" + beginday + "' and date(time) <= '" + endday + "'"
    cur.execute(sql)
    results = cur.fetchall()
    ui_list = uipairs.tolist()

    new_uipairs = np.array([])
    new_feat = np.array([])
    new_label = np.array([])
    for row in results:
        uipair = str(row[0]) + ',' + str(row[1])
        if uipair in ui_list:
            i = ui_list.index(uipair)
            new_feat.add(feat[i])
            new_uipairs.add(uipairs[i])
            new_label.add(label[i])

    posnum = len([a for a in new_label if a == 1])  #正样本
    negnum = len(new_label) - posnum
    print 'after rule2 clean,sample posenum is ' + str(posnum) + ' and negnum is ' + str(negnum)

    return new_uipairs, new_feat, new_label


#规则2 是否已经购买过
def rule3(uipairs, lastday, feat=0, label=0):
    conn = MySQLdb.connect(host=sqlhost, user=sqluser, passwd=sqlpswd, db=sqldb, charset='utf8', port=3306)
    cur = conn.cursor()
    sql = "select DISTINCT user_id,item_id from train_user_target_item_new where behavior_type = 4 and date(time) = '" + lastday + "'"
    cur.execute(sql)
    results = cur.fetchall()
    rule2 = []

    for row in results:
        uipair = str(row[0]) + ',' + str(row[1])
        rule2.append(uipair)

    for i in range(0, len(uipairs)):
        uipair = str(int(uipairs[i][0])) + ',' + str(int(uipairs[i][1]))
        if uipair in rule2:
            uipairs = np.delete(uipairs, i, 0)
            label = np.delete(label, i, 0)
            feat = np.delete(feat, i, 0)

    posnum = len([a for a in label if a == 1])  #正样本
    negnum = len(label) - posnum
    print 'after rule2 clean,sample posenum is ' + str(posnum) + ' and negnum is ' + str(negnum)


#规则3 没有购买行为,点击行为异常
def rule4(uipairs, feat=0, label=0):
    conn = MySQLdb.connect(host=sqlhost, user=sqluser, passwd=sqlpswd, db=sqldb, charset='utf8', port=3306)
    cur = conn.cursor()
    sql = 'select DISTINCT user_id from train_user_target_item_new where behavior_type = 4'
    cur.execute(sql)
    results = cur.fetchall()
    rule3 = set()

    for row in results:
        rule3.add(row[0])

    for i in range(0, len(uipairs)):
        user_id = int(uipairs[i].splite(',')[0])
        if user_id not in rule3:
            uipairs = np.delete(uipairs, i, 0)
            label = np.delete(label, i, 0)
            feat = np.delete(feat, i, 0)

    posnum = len([a for a in label if a == 1])  #正样本
    negnum = len(label) - posnum
    print 'after rule3 clean,sample posenum is ' + str(posnum) + ' and negnum is ' + str(negnum)


def rulepool4train(uipairs, lastday, feat, label):
    #rule1(uipairs,lastday,feat,label)
    #rule2(uipairs,lastday,feat,label)
    #rule3(uipairs,lastday,feat,label)

    posnum = len([a for a in label if a == 1])  #正样本
    negnum = len(label) - posnum
    print 'after rule pool clean,sample posenum is ' + str(posnum) + ' and negnum is ' + str(negnum)

    return uipairs, feat, label


#规则池
def rulepool4result(uipairs, lastday, probs, threshold):
    uipairs
    cleanedpairs = []
    cleaningpairs = set()
    i = 0
    for prob in probs:
        if prob < threshold:
            cleaningpairs.add(uipairs[i])
        else:
            cleanedpairs.append(uipairs[i])
        i += 1

    rule1(cleaningpairs, lastday)
    rule2(cleaningpairs, lastday)
    rule3(cleaningpairs, lastday)
    cleanedpairs.extend(cleaningpairs)
    return cleanedpairs


#预测分类
def predict(estimators, file4feat):
    uipair, feat = prepare4onlinepredict(file4feat)
    for k in estimators.keys():
        label_pred = estimators[k].predict(feat)
        #rulepool4result(uipairs,lastday,probs,0.6)
        saveres('predict_result' + k + '.csv', uipair, label_pred)
        num = len([a for a in label_pred if a == 1])  #推荐列表长度
        print 'num of pos samples:' + str(num)


#本地测试
def offtest(estimators, file4feat, file4label):
    starttime = datetime.datetime.now()
    uipair, feat, label, lastdayposnum = prepare4offlinepredict(file4feat, file4label)
    starttime = endtime(starttime, 'prepare for offlinepredict')

    for k in estimators.keys():
        label_pred = estimators[k].predict(feat)
        # print k+' f1_score:'+str(f1_score(label, label_pred))
        # print k+' precision_score:'+str(precision_score(label, label_pred))
        # print k+' recall_score:'+str(recall_score(label, label_pred))

        i = 0
        n = 0  #预测对的
        for p in label_pred:
            if label[i] == 1 and p == 1:
                n += 1
            i += 1

        num = len([a for a in label_pred if a == 1])  #推荐列表长度
        if num > 0:
            p = float(n) / num
            r = float(n) / lastdayposnum
            f1 = 2 * (p * r) / (p + r)
            print 'precision_score:' + str(p) + ' recall_score:' + str(r) + ' f1_score:' + str(f1)
        print 'n:' + str(n) + ' num:' + str(num) + ' lastdayposnum:' + str(lastdayposnum)

        saveres('offline_result.csv', uipair, label_pred)
        starttime = endtime(starttime, k + ' offlinepredict')


path = os.path.abspath(os.path.dirname(os.path.dirname(__file__))) + '/source/feature'
print path
os.chdir(path)  # change dir to '~/files'

#joblib.dump(estimators, 'D:/tianchi/feat/model/11-16-11-19.pkl')
#estimators = joblib.load('D:/tianchi/feat/model/1.pkl') 

starttime = datetime.datetime.now()
print '\n------train section------'
#estimators = train('2014-11-18-2014-12-17-matrix.csv', '2014-11-18-2014-12-17-lastday-label.csv','2014-12-16')#
estimators = train('2014-11-18-2014-12-16-matrix.csv', '2014-11-18-2014-12-16-lastday-label.csv', '2014-12-15')  #
starttime = endtime(starttime, 'train')

print '\n------test section------'
offtest(estimators, '2014-11-18-2014-12-17-matrix.csv', '2014-11-18-2014-12-17-lastday-label.csv')  #
starttime = endtime(starttime, 'test')

# print '\n------train section------'
# estimators = train('2014-11-18-2014-12-18-matrix.csv', '2014-11-18-2014-12-18-lastday-label.csv','2014-12-17')#
# starttime = endtime(starttime, 'train')
#
# print '\n------predict secetion------'
# predict(estimators, '2014-11-18-2014-12-19-matrix.csv')
# starttime = endtime(starttime, 'predict')