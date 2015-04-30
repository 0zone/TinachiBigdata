# coding=utf-8
import os

__author__ = 'jinyu'
import types
from datetime import *
import numpy as np

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


# 按日期切分文件
# 生成split文件 >=begin    <end
# 生成lastday文件   =end
def split_file_by_date(raw_file_path, begin, end):
    print "\nspliting file by date..."
    interval_days = date_interval_days(begin, end)

    split_file_path = "./temp/"+begin + "-" + end + "-" + "split-" + raw_file_path
    lastday_file_path = "./feature/" + begin + "-" + end + "-" + "lastday-label.csv"
    split_file = open(split_file_path, 'w')
    lastday_file = open(lastday_file_path, 'w')

    raw_file = open(raw_file_path)
    raw_file.readline()     # 读出栏位名
    for line in raw_file:
        entrys = line.split(delimiter)
        date_delta = date_interval_days(begin, entrys[5])

        if (date_delta < interval_days) and (date_delta >= 0):
            split_file.write(line)

        if date_interval_days(end, entrys[5]) == 0:
            lastday_file.write(line)

    raw_file.close()
    split_file.close()
    lastday_file.close()

    print "split file by date completed"
    return split_file_path, lastday_file_path


def parse_line(line):
    user_id, item_id, behavior_type, user_geohash, item_category, date = line.split(delimiter)
    date = datetime.strptime(date.strip() + ":0:0", "%Y-%m-%d %H:%M:%S")
    return user_id, item_id, behavior_type, user_geohash, item_category, date


# 加载特征  返回列索引和所有特征
def load_features(features_file_path):
    features_file = open(features_file_path)

    column_index = {}   # {"column_name", index}
    features = {}       # {"id", []}

    column_name = features_file.readline()
    index = -1
    for name in column_name.split(delimiter)[0:-1]:
        column_index[name] = index
        index += 1

    for line in features_file:
        entrys = line.split(delimiter)
        id = entrys[0]
        features_list = entrys[1:-1]   # 最后一列为换行 故取到-1
        features[id] = features_list
    print "load " + features_file_path + " completed"
    features_file.close()

    return column_index, features


def load_uc_features(uc_features_file_path):
    uc_features_file = open(uc_features_file_path)

    column_index = {}   # {"column_name", index}
    uc_features = {}    # {"id", []}

    column_name = uc_features_file.readline()
    index = -2  # 忽略user_id 和 cate_id
    for name in column_name.split(delimiter)[0:-1]:
        column_index[name] = index
        index += 1

    for line in uc_features_file:
        entrys = line.split(delimiter)
        category_id = entrys[1]
        uc_features_list = entrys[2:-1]   # 最后一列为换行 故取到-1
        uc_features[category_id] = uc_features_list
    print "load " + uc_features_file_path + " completed"
    uc_features_file.close()

    return column_index, uc_features


# 加载特征矩阵数据      返回 ui_array和train_array
def load_matrix(matrix_file_path):
    matrix_file = open(matrix_file_path)

    matrix = np.loadtxt(matrix_file, skiprows=1, delimiter=',')
    row, column = matrix.shape
    print "matrix size is : ", row, ",", column

    train_ui = matrix[:, 0:2]
    train_x = matrix[:, 2:column]

    matrix_file.close()
    print "load " + matrix_file_path + " completed"

    return train_ui, train_x


# 加载标签数据        返回 label_array
def load_label(label_file_path, ui_pair_array):
    label = {}
    label_list = []
    positive_count = 0

    label_file = open(label_file_path)

    for line in label_file:
        entrys = line.split(delimiter)
        ui_id = delimiter.join(entrys[0:2])
        if entrys[2] == '4':
            label[ui_id] = 1
            positive_count += 1

    label_file.close()

    # 遍历ui_pair,初始化每个ui的label
    for ui_pair in ui_pair_array:
        ui_id = str(int(ui_pair[0])) + "," + str(int(ui_pair[1]))
        label_list.append(label.get(ui_id, 0))

    return positive_count, np.asarray(label_list)


# 根据字段名返回相应字段对应的特征
def merge_features_by_dict(tmp_features, other_features, feat_name, useful_feat_name):
    features = {}
    useful_features = {}
    # 相同的会被覆盖为一个
    all_featurs = dict(tmp_features)
    all_featurs.update(other_features)

    for name in feat_name:
        features[name] = all_featurs[name]
    for name in useful_feat_name:
        useful_features[name] = all_featurs[name]

    return features, useful_features


def get_features_str_by_dict(features, feat_name):
    features_str = ''
    for name in feat_name:
        features_str += str(features[name]) + ','

    return features_str


# 获取item  category字典
def get_item_category_dict(raw_item_file_path):
    item_category_dict = {}

    raw_item_file = open(raw_item_file_path)
    for line in raw_item_file:
        line_entrys = line.strip().split(delimiter)
        item_id = line_entrys[0]
        category_id = line_entrys[2]
        item_category_dict[item_id] = category_id

    raw_item_file.close()
    return item_category_dict


def filter_matrix(rule_file_path, matrix_file_path):
    rule_ui = set()
    rule_file = open(rule_file_path)

    for line in rule_file:
        line_entrys = line.strip().split(delimiter)
        ui_id = line_entrys[0] + delimiter + line_entrys[1]
        rule_ui.add(ui_id)
    rule_file.close()

    matrix_with_rule_path = rule_file_path + "_" + matrix_file_path

    matrix_file = open(matrix_file_path)
    matrix_with_rule = open(matrix_with_rule_path, 'w')
    matrix_with_rule.write(matrix_file.readline())      # 列名写入
    for line in matrix_file:
        line_entrys = line.strip().split(delimiter)
        ui_id = line_entrys[0] + delimiter + line_entrys[1]

        if ui_id in rule_ui:
            matrix_with_rule.write(line)
    rule_file.close()


def insert_label(features_file_path, label_file_path, label_column_index=1):

    label_file = open(label_file_path)
    label_set = set()
    for line in label_file:
        line_entrys = line.split(delimiter)
        ui_id = delimiter.join(line_entrys[0:2])
        if line_entrys[2] == '4':
            label_set.add(ui_id)
    label_file.close()

    features_with_label_file_path = features_file_path.split(".")[0] + "_withlabel.csv"
    features_with_label_file = open(features_with_label_file_path, 'w')
    features_file = open(features_file_path)
    for line in features_file:
        line_entrys = line.split(delimiter)
        ui_id = delimiter.join(line_entrys[0:2])
        ui_label = "0"
        if ui_id in label_set:
            ui_label = "1"
        line_entrys.insert(label_column_index, ui_label)
        features_with_label_file.write(delimiter.join(line_entrys))
    features_file.close()
    features_with_label_file.close()


# path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))+'\\source\\feature'
# os.chdir(path)  # change dir to '~/files'
# insert_label("2014-11-18-2014-12-17-uifeat.csv", "2014-11-18-2014-12-17-lastday-label.csv", 2)
