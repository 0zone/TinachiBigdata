from sklearn import datasets
from sklearn import linear_model
from sklearn import preprocessing
from sklearn.metrics import f1_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from numpy import *
from sklearn.ensemble import ExtraTreesClassifier

digits = datasets.load_digits()
iris = datasets.load_iris()
clf = linear_model.LogisticRegression()

# x = array( [[1.,1.],[2.,3.],[10.,9.],[5.,2.]], dtype=float32)
# x1 = preprocessing.normalize(x)
# y = array( [1,1,0,0])
# xx = array( [[1.,0.],[10.,0.],[5.,3.],[9.,15.]], dtype=float32)
# yy = array( [0,0,0,1])
# xx1 = preprocessing.normalize(xx)
x = array( [[1.,1.,2.,3.,4.,5.,6.,7.],[2.,3.,10.,3,4.,6.,12.,1.],[10.,9.,2.,4.,1.,9.,4.,12.],[5.,2.,4.,12,3.,7.,5.,42.]], dtype=float32)
y = array( [1,1,0,0])
clf.fit(x, y)#save one 
#label = clf.predict(xx1)
#prob = clf.predict_proba(xx1)

#print prob
param = clf.coef_
print param

clf = linear_model.LogisticRegression(class_weight = {1:50,0:1},penalty = 'l1',C = 0.01)
clf.fit(x, y)#save one 

param = clf.coef_
print param

x = array( [[1.,1.,2.,3.,4.,5.,6.,7.],[2.,3.,10.,3,4.,6.,12.,1.],[10.,9.,2.,4.,1.,9.,4.,12.],[5.,2.,4.,12,3.,7.,5.,42.]], dtype=float32)
y = array( [1,1,0,0])
clf = ExtraTreesClassifier()
clf.fit(x, y)
clf.feature_importances_  

