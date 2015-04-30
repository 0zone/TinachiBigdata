# coding=utf-8
import os

__author__ = 'jinyu'
import types
from datetime import *

delimiter = ','
# 生成字典的key值
def get_features_key(features):
    features_key_str = ''
    for k, v in features.iteritems():
        if (type(v) is not types.ListType) and (type(v) is not types.DictType):
            features_key_str += str(k) + ','

    return features_key_str


# 解析日期
def parse_date(raw_date):
    entry_date = raw_date
    year, month, day = entry_date.split(" ")[0].split("-")
    return int(year), int(month), int(day)


# 计算日期间隔
def date_interval_days(begin, end):
    begin_date = date(*parse_date(begin))
    end_date = date(*parse_date(end))
    return (end_date - begin_date).days


# 文件排序
def generate_sortedfile(origin_file_path, new_file_path, sort_column1=0, sort_column2=1):
    origin_file = open(origin_file_path)

    entrys = origin_file.readlines()
    entrys.sort(key=lambda x: (x.split(",")[sort_column1], x.split(",")[sort_column2]))
    sorted_file = open(new_file_path, "w")
    for i in entrys:
        sorted_file.write(i)
    sorted_file.close()
    origin_file.close()


# 按日期切分文件   >=begin_date    <end_date
def split_file_by_date(raw_file_path, begin, end):
    interval_days = date_interval_days(begin, end)

    split_file_path = begin + "_" + end + "_" + raw_file_path
    split_file = open(split_file_path, 'w')

    raw_file = open(raw_file_path)
    # raw_file.readline()     # 读出栏位名
    for line in raw_file:
        entry = line.split(",")
        date_delta = date_interval_days(begin, entry[5])

        if (date_delta < interval_days) and (date_delta >= 0):
            split_file.write(line)

    raw_file.close()
    split_file.close()


def parse_line(line):
    user_id, item_id, behavior_type, user_geohash, item_category, date = line.split(delimiter)
    date = datetime.strptime(date.strip() + ":0:0", "%Y-%m-%d %H:%M:%S")
    return user_id, item_id, behavior_type, user_geohash, item_category, date


def load_features(features_file_path, features, key_index=0):
    features_file = open(features_file_path)

    for line in features_file:
        line_entrys = line.split(delimiter)
        key_id = line_entrys[key_index]
        features_list = line_entrys[1:-1]
        features[key_id] = delimiter.join(features_list)
    print "load users_features completed"
    features_file.close()
    return features
