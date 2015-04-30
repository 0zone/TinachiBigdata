# coding=utf-8
from extract_ucpairs_features import extract_uc_features
from extract_categorys_features import extract_categorys_features
from extract_users_features import extract_user_features
from extract_items_features import extract_items_features
from extract_uipairs_features import extract_ui_features
from generate_matrix import generate_matrix, generate_matrix_with_label
from util import *
from data_processing import *
import datetime
import os

path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))+'\\source'
os.chdir(path)  # change dir to '~/files'

raw_user_file_path = "tianchi_mobile_recommend_train_user.csv"
raw_item_file_path = "tianchi_mobile_recommend_train_item.csv"
filtered_file_path = "filtered_unknownitem_tianchi_mobile_recommend_train_user.csv"
# users_feat_file_path = "./feature/2014-11-18-2014-12-17-userfeat.csv"
# items_feat_file_path = "./feature/2014-11-18-2014-12-17-itemfeat.csv"
# ui_feat_file_path = "./feature/2014-11-18-2014-12-17-uifeat.csv"
# categorys_feat_file_path = "./feature/2014-11-18-2014-12-17-categoryfeat.csv"
# uc_feat_file_path = "./feature/2014-11-18-2014-12-17-ucfeat.csv"
# lastday_file_path = "./feature/2014-11-18-2014-12-17-lastday-label.csv"


def building_files(begin_date, end_date):
    print "------------------------"
    print begin_date + "---" + end_date
    start_time = datetime.datetime.now()

    split_file_path, lastday_file_path = split_file_by_date(filtered_file_path, begin_date, end_date)                  # 切分文件
    users_feat_file_path = extract_user_features(split_file_path, begin_date, end_date)             # 提取用户特征
    items_feat_file_path = extract_items_features(split_file_path, begin_date, end_date)            # 提取商品特征
    ui_feat_file_path = extract_ui_features(split_file_path, begin_date, end_date)                  # 提取ui特征
    categorys_feat_file_path = extract_categorys_features(split_file_path, begin_date, end_date)    # 提取商品特征
    uc_feat_file_path = extract_uc_features(split_file_path, begin_date, end_date)                  # 提取ui特征

    matrix_file_path = generate_matrix_with_label(raw_item_file_path, ui_feat_file_path, users_feat_file_path, items_feat_file_path, categorys_feat_file_path, uc_feat_file_path, lastday_file_path, begin_date, end_date)     # 生成矩阵

    print begin_date + "---" + end_date + "运行时间:" + str((datetime.datetime.now() - start_time).seconds) + "s"
    return matrix_file_path


begin = datetime.datetime(2014, 12, 1).date()
end = datetime.datetime(2014, 12, 17).date()
window_size = 7

all_matrix_file_path = "./feature/all-" + begin.strftime("%Y-%m-%d") + "-" + end.strftime("%Y-%m-%d") + "-matrix.csv"
all_matrix_file = open(all_matrix_file_path, 'w')

# 1~16  16号打标签  线下预测17号 range(0, 9)
for i in range(0, 9):
    begin_date = begin + datetime.timedelta(days=i)
    end_date = begin_date + datetime.timedelta(days=window_size)
    matrix_file_path = building_files(begin_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    matrix_file = open(matrix_file_path)
    for line in matrix_file:
        all_matrix_file.write(line)
    matrix_file.close()

all_matrix_file.close()
