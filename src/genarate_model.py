# coding=utf-8
from array import array
import os

__author__ = 'jinyu'


import numpy as np
import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn import datasets
from sklearn.preprocessing import StandardScaler


delimiter = ','


def model(matrix_file_path):
    matrix_file = open(matrix_file_path)

    trainSet = np.loadtxt(matrix_file, delimiter=delimiter)
    row, column = trainSet.shape
    X = trainSet[:, 2:column-1]
    y = trainSet[:, column-1]
    matrix_file.close()

    print "train"
    clf_l1_LR = LogisticRegression(C=1.0, penalty='l1', tol=0.01)
    clf_l1_LR.fit(X, y)
    X.clear
    y.clear
    print clf_l1_LR.coef_



path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))+'\\source'
os.chdir(path)  # change dir to '~/files'

matrix_file_path = "matrix.csv"
model(matrix_file_path)

