# coding=utf-8
__author__ = 'jinyu'

import os
from util import load_features, load_uc_features

delimiter = ','


# 生成训练矩阵
def generate_matrix(raw_item_file_path, uipairs_features_file_path, users_features_file_path, items_features_file_path, categorys_features_file_path, ucpairs_features_file_path, begin_date, end_date):
    print "\n" + begin_date + "---" + begin_date + "generating matrix..."

    users_column_index, users_features = load_features(users_features_file_path)
    items_column_index, items_features = load_features(items_features_file_path)
    categorys_column_index, categorys_features = load_features(categorys_features_file_path)
    uc_column_index, uc_features = load_uc_features(ucpairs_features_file_path)

    uipairs_features_file = open(uipairs_features_file_path)

    matrix_file_path = "./feature/" + begin_date + "-" + end_date + "-matrix.csv"
    matrix_file = open(matrix_file_path, 'w')

    # 加载item category字典
    item_category_dict = {}
    raw_item_file = open(raw_item_file_path)
    for line in raw_item_file:
        line_entrys = line.strip().split(delimiter)
        item_id = line_entrys[0]
        category_id = line_entrys[2]
        item_category_dict[item_id] = category_id
    raw_item_file.close()

    # 读取列名
    users_features_file = open(users_features_file_path)
    items_features_file = open(items_features_file_path)
    categorys_features_file = open(categorys_features_file_path)
    ucpairs_features_file = open(ucpairs_features_file_path)
    ui_column_name = uipairs_features_file.readline().split(delimiter)[:-1] + \
                     users_features_file.readline().split(delimiter)[1:-1] + \
                     items_features_file.readline().split(delimiter)[1:-1] + \
                     categorys_features_file.readline().split(delimiter)[1:-1] + \
                     ucpairs_features_file.readline().split(delimiter)[2:-1]

    matrix_file.write(delimiter.join(ui_column_name) + "\n")
    users_features_file.close()
    items_features_file.close()
    categorys_features_file.close()
    ucpairs_features_file.close()

    for line in uipairs_features_file:
        line_entrys = line.split(delimiter)
        user_id = line_entrys[0]
        item_id = line_entrys[1]

        # matrix_line = delimiter.join(line_entrys[:-1]) + delimiter + \
        #               delimiter.join(users_features[user_id]) + delimiter + \
        #               delimiter.join(items_features[item_id]) + \
        #               delimiter.join(categorys_features[item_id]) + \
        #               delimiter.join(uc_features[ui_id]) + "\n"
        matrix_line = line_entrys[:-1] + \
                      users_features[user_id] + \
                      items_features[item_id] + \
                      categorys_features[item_category_dict[item_id]] + \
                      uc_features[item_category_dict[item_id]]

        matrix_file.write(delimiter.join(matrix_line) + "\n")

    matrix_file.close()
    uipairs_features_file.close()
    print "generate matrix completed\n"

    return matrix_file_path


# 生成带标签的训练矩阵
def generate_matrix_with_label(raw_item_file_path, uipairs_features_file_path, users_features_file_path, items_features_file_path, categorys_features_file_path, ucpairs_features_file_path, label_file_path, begin_date, end_date):
    print "\n" + begin_date + "---" + end_date + "generating matrix with label..."

    users_column_index, users_features = load_features(users_features_file_path)
    items_column_index, items_features = load_features(items_features_file_path)
    categorys_column_index, categorys_features = load_features(categorys_features_file_path)
    uc_column_index, uc_features = load_uc_features(ucpairs_features_file_path)

    uipairs_features_file = open(uipairs_features_file_path)

    matrix_file_path = "./feature/" + begin_date + "-" + end_date + "-matrix-label.csv"
    matrix_file = open(matrix_file_path, 'w')

    # 加载item category字典
    item_category_dict = {}
    raw_item_file = open(raw_item_file_path)
    for line in raw_item_file:
        line_entrys = line.strip().split(delimiter)
        item_id = line_entrys[0]
        category_id = line_entrys[2]
        item_category_dict[item_id] = category_id
    raw_item_file.close()

    label_set = set()
    label_file = open(label_file_path)
    for line in label_file:
        line_entrys = line.split(delimiter)
        ui_id = delimiter.join(line_entrys[0:2])
        if line_entrys[2] == '4':
            label_set.add(ui_id)
    label_file.close()

    # 读取列名
    users_features_file = open(users_features_file_path)
    items_features_file = open(items_features_file_path)
    categorys_features_file = open(categorys_features_file_path)
    ucpairs_features_file = open(ucpairs_features_file_path)
    ui_column_name = uipairs_features_file.readline().split(delimiter)[:-1] + \
                     users_features_file.readline().split(delimiter)[1:-1] + \
                     items_features_file.readline().split(delimiter)[1:-1] + \
                     categorys_features_file.readline().split(delimiter)[1:-1] + \
                     ucpairs_features_file.readline().split(delimiter)[2:-1]

    # matrix_file.write(delimiter.join(ui_column_name) + ",label\n")
    users_features_file.close()
    items_features_file.close()
    categorys_features_file.close()
    ucpairs_features_file.close()

    for line in uipairs_features_file:
        line_entrys = line.split(delimiter)
        user_id = line_entrys[0]
        item_id = line_entrys[1]

        # matrix_line = delimiter.join(line_entrys[:-1]) + delimiter + \
        #               delimiter.join(users_features[user_id]) + delimiter + \
        #               delimiter.join(items_features[item_id]) + \
        #               delimiter.join(categorys_features[item_id]) + \
        #               delimiter.join(uc_features[ui_id]) + "\n"
        matrix_line = line_entrys[:-1] + \
                      users_features[user_id] + \
                      items_features[item_id] + \
                      categorys_features[item_category_dict[item_id]] + \
                      uc_features[item_category_dict[item_id]]

        label = "0"
        if (user_id+","+item_id) in label_set:
            label = "1"
        matrix_file.write(delimiter.join(matrix_line) + "," + label + "\n")

    matrix_file.close()
    uipairs_features_file.close()
    print "generate matrix with label completed\n"

    return matrix_file_path

# path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))+'\\source'
# os.chdir(path)  # change dir to '~/files'
#
# uipairs_features_file_path = "./feature/2014-12-9-2014-12-18-uifeat.csv"
# users_features_file_path = "./feature/2014-12-9-2014-12-18-userfeat.csv"
# items_features_file_path = "./feature/2014-12-9-2014-12-18-itemfeat.csv"
#
# generate_matrix(uipairs_features_file_path, users_features_file_path, items_features_file_path, "2014-12-9", "2014-12-18")