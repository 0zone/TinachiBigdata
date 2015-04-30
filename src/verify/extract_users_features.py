# coding=utf-8
__author__ = 'jinyu'

import os
from datetime import datetime,timedelta
from util import *


buy_behaviour_type = '4'
cart_behaviour_type = '3'
favorite_behaviour_type = '2'
click_behaviour_type = '1'
delimiter = ','
days = 0
lastday = 0

# 遍历文件时候得到的属性
basic_feature_behaviour = {'4': ['buy_counts', 'buy_brands_list', 'buy_categorys_list'],
                           '3': ['cart_counts', 'cart_brands_list', 'cart_categorys_list'],
                           '2': ['favor_counts', 'favor_brands_list', 'favor_categorys_list'],
                           '1': ['click_counts', 'click_brands_list', 'click_categorys_list']}

# 基础属性可以直接使用的
basic_useful_feat_index = [0]

# 对基础属性加工时候得到的属性
other_feature_behaviour = {'4': ['buy_brands_count', 'buy_categorys_count'],
                           '3': ['cart_brands_count', 'cart_categorys_count'],
                           '2': ['favor_brands_count', 'favor_categorys_count'],
                           '1': ['click_brands_count', 'click_categorys_count']}

def initial_tmp_features(tmp_features):
    # ##基础属性
    for featurenames in basic_feature_behaviour.values():
        tmp_features[featurenames[0]] = 0    # xxx_counts # 销量 # 购物车量 # 收藏量 # 点击量
        tmp_features[featurenames[1]] = []   # xxx_brands_list {user:count} # 购物车用户 # 消费用户
        tmp_features[featurenames[2]] = []   # xxx_categorys_list

    tmp_features['active_days_list'] = []
    tmp_features['buy_days_list'] = []


def initial_user_features(other_features):
    # ##扩展属性
    for featurenames in  other_feature_behaviour.values():
        other_features[featurenames[0]] = 0    # xxx_brands_count
        other_features[featurenames[1]] = 0    # xxx_categorys_count

    other_features['active_days_count'] = 0
    other_features['buy_days_count'] = 0

    # 购买转化率
    other_features['buy_per_cart'] = 0
    other_features['buy_per_favor'] = 0
    other_features['buy_per_click'] = 0
    other_features['brand_buy_per_cart'] = 0
    other_features['brand_buy_per_favor'] = 0
    other_features['brand_buy_per_click'] = 0

    # 比值特征
    other_features['buy_days_count_per_active_days_count'] = 0
    other_features['buy_counts_per_buy_days_count'] = 0
    other_features['active_days_count_per_days'] = 0

    return other_features


def extract_user_features(train_file_path, begin_date, end_date):

    generate_sortedfile(train_file_path, "sorted_by_user-" + train_file_path, 0)
    train_file = open("sorted_by_user-" + train_file_path)

    filename = "./feature/" + begin_date + "-" + end_date + "-userfeat.csv"
    user_features_file = open(filename, 'w')

    global days, lastday
    days = date_interval_days(begin_date, end_date)
    lastday = datetime.strptime(end_date, "%Y-%m-%d").date() - timedelta(days=1)

    # tmp_features初始化
    tmp_features = {}
    initial_tmp_features(tmp_features)


    other_features = get_other_basic_user_features(tmp_features)    # 获取用户其他特征
    all_features = merge_features(tmp_features, other_features)
    print get_features_key(all_features)
    user_features_file.write("user_id" + "," + get_features_key(all_features) + "\n")


    initial_tmp_features(tmp_features)

    pre_user_id = train_file.readline().split(delimiter)[0]     # 获取第一行的user_id
    train_file.seek(0)
    for line in train_file:
        user_id, item_id, behavior_type, user_geohash, item_category, date = parse_line(line)

        # 如果当前pre_user_id和读取到的user_id不一样则输出当前user_features并置空
        if not user_id == pre_user_id:
            other_features = get_other_basic_user_features(tmp_features)    # 获取用户其他特征
            all_features = merge_features(tmp_features, other_features)
            user_features_file.write(pre_user_id + "," + get_user_features_str(all_features) + "\n")  # 输出当前user_features
            initial_tmp_features(tmp_features)    # 初始化置空user_features
            if pre_user_id=='70412049':
                print all_features["click_counts"]
        # 计算当前user基本特征
        tmp_features['active_days_list'].append(date.date())

        # 行为基本特征计算           'buy_counts','buy_brands_list','buy_categorys_list'
        xxx_counts = basic_feature_behaviour[behavior_type][0]
        xxx_brands_list = basic_feature_behaviour[behavior_type][1]
        xxx_categorys_list = basic_feature_behaviour[behavior_type][2]
        # eachday_xxx_counts  = basic_feature_behaviour[behavior_type][3]

        tmp_features[xxx_counts] += 1
        tmp_features[xxx_brands_list].append(item_id)
        tmp_features[xxx_categorys_list].append(item_category)

        if behavior_type == buy_behaviour_type:
            tmp_features['buy_days_list'].append(date.date())

        pre_user_id = user_id

    other_features = get_other_basic_user_features(tmp_features)    # 获取用户其他特征
    all_features = merge_features(tmp_features, other_features)
    user_features_file.write(pre_user_id + "," + get_user_features_str(all_features) + "\n")

    user_features_file.close()
    train_file.close()


def get_other_basic_user_features(tmp_features):
    other_features = {}
    initial_user_features(other_features)

    for behaviour in ['1', '2', '3', '4']:
        xxx_brands_count = other_feature_behaviour[behaviour][0]
        xxx_categorys_count = other_feature_behaviour[behaviour][1]

        xxx_brands_list = basic_feature_behaviour[behaviour][1]
        xxx_categorys_list = basic_feature_behaviour[behaviour][2]

        other_features[xxx_brands_count] = float(len(set(tmp_features[xxx_brands_list])))
        other_features[xxx_categorys_count] = float(len(set(tmp_features[xxx_categorys_list])))


    other_features['active_days_count'] = float(len(set(tmp_features['active_days_list'])))
    other_features['buy_days_count'] = float(len(set(tmp_features['buy_days_list'])))

    # 购买转化率
    other_features['buy_per_cart'] = float(tmp_features['buy_counts']) / (tmp_features['cart_counts'] + 1)
    other_features['buy_per_favor'] = float(tmp_features['buy_counts']) / (tmp_features['favor_counts'] + 1)
    other_features['buy_per_click'] = float(tmp_features['buy_counts']) / (tmp_features['click_counts'] + 1)

    other_features['brand_buy_per_cart'] = float(other_features['buy_brands_count']) / (other_features['cart_brands_count'] + 1)
    other_features['brand_buy_per_favor'] = float(other_features['buy_brands_count']) / (other_features['favor_brands_count'] + 1)
    other_features['brand_buy_per_click'] = float(other_features['buy_brands_count']) / (other_features['click_brands_count'] + 1)

    #比值特征
    other_features['active_days_count_per_days'] = float(other_features['active_days_count']) / (days + 1)
    other_features['buy_days_count_per_active_days_count'] = float(other_features['buy_days_count']) / (other_features['active_days_count'] + 1)
    other_features['buy_counts_per_buy_days_count'] = float(tmp_features['buy_counts']) / (other_features['buy_days_count'] + 1)

    return other_features


def merge_features(tmp_features, other_features):
    item_features = {}

    for behaviour in ['1', '2', '3', '4']:
        for userful_index in basic_useful_feat_index:
            feature_name = basic_feature_behaviour[behaviour][userful_index]
            item_features[feature_name] = tmp_features[feature_name]

    item_features.update(other_features)
    return item_features


def get_user_features_str(user_features):
    user_features_str = ''
    for k, v in user_features.iteritems():
        if type(v) is not types.ListType:
            user_features_str += str(v) + ','

    return user_features_str

path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))+'\\source'
os.chdir(path)  # change dir to '~/files'
train_file_path = "validation_filtered_unknownitem_tianchi_mobile_recommend_train_user.csv"
extract_user_features(train_file_path, "2014-12-9", "2014-12-19")


