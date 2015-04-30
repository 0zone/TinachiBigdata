#!/usr/bin/python
# -*- coding: utf-8 -*-

import os 
from util import generate_sortedfile, parse_line, get_features_key
from datetime import datetime, timedelta
import types

buy_behaviour_type = '4'
cart_behaviour_type = '3'
favor_behaviour_type = '2'
click_behaviour_type = '1'
delimiter = ','
lastday = 0
#遍历文件时候得到的属性
basic_feature_behaviour = {'4':['c_buy_counts','c_buy_user','c_user_first_buy_time','c_eachday_buy_counts'],
                           '3':['c_cart_counts','c_cart_user','c_user_first_cart_time','c_eachday_cart_counts'],
                           '2':['c_favor_counts','c_favor_user','c_user_first_favor_time','c_eachday_favor_counts'],
                           '1':['c_click_counts','c_click_user','c_user_first_click_time','c_eachday_click_counts']}

#基础属性可以直接使用的
basic_useful_feat_index = [0,1]

#对基础属性加工时候得到的属性
other_feature_behaviour = {'4':['c_lastday_buy_counts','c_total_buy_people','c_average_buy'],
                           '3':['c_lastday_cart_counts','c_total_cart_people','c_average_cart'],
                           '2':['c_lastday_favor_counts','c_total_favor_people','c_average_favor'],
                           '1':['c_lastday_click_counts','c_total_click_people','c_average_click']}

def initial_tmp_features(tmp_features):

    ###基础属性
    for featurename in  basic_feature_behaviour.values():
        tmp_features[featurename[0]] = 0    #xxx_counts # 销量 # 购物车量 # 收藏量 # 点击量
        tmp_features[featurename[1]] = {}   #xxx_user {user:count} # 用户xxx的次数
        tmp_features[featurename[2]] = {}   #user_first_xxx_time {user:first_time}  #用户第一次xxx的时间
        tmp_features[featurename[3]] = {}   #eachday_xxx_counts {time:count}  #每天xxx的数量
       

        
def initial_category_features(other_features):

    ###扩展属性
    for featurename in  other_feature_behaviour.values():
        other_features[featurename[0]] = 0    #lastday_xxx_counts #最后一天行为次数
        other_features[featurename[1]] = 0    #total_xxx_people #总购物车人数 
        other_features[featurename[2]] = 0    #average_xxx  #平均xxx量
 
    other_features['c_buy_per_cart'] = 0
    other_features['c_buy_per_favorite'] = 0
    other_features['c_buy_per_click'] = 0
    other_features['c_people_buy_per_cart'] = 0
    other_features['c_people_buy_per_favorite'] = 0
    other_features['c_people_buy_per_click'] = 0

    # 比值特征
    other_features['c_comeback_rate'] = 0
    other_features['c_jump_rate'] = 0
    other_features['c_active_rate'] = 0

def extract_categorys_features(train_file_path,begin_date,end_date):
    # 按商品id排序
    print "\n" + begin_date + "---" + end_date + "extracting categorys features..."
    generate_sortedfile(train_file_path, "temp/sorted_by_category-" + train_file_path.split('/')[-1], 4)

    train_file = open("temp/sorted_by_category-" + train_file_path.split('/')[-1])
    categorys_feat_file_path = "./feature/" + begin_date + "-" + end_date + "-categoryfeat.csv"
    categorys_features_file = open(categorys_feat_file_path, 'w')

    tmp_features = {}
    initial_tmp_features(tmp_features)

    # 输出栏位名
    other_features = get_other_basic_category_features(tmp_features)    # 获取其他特征
    all_features = merge_features(tmp_features, other_features)
    # print "category_featurename is :\n",get_features_key(all_features)
    categorys_features_file.write("category_id" + "," + get_features_key(all_features) + "\n")
    initial_tmp_features(tmp_features)

    global lastday
    lastday = datetime.strptime(end_date, "%Y-%m-%d").date() - timedelta(days=1)
    
    pre_category_id = train_file.readline().split(delimiter)[4]  # 获取第一行的category_id
    train_file.seek(0)
    for line in train_file:
        user_id, item_id, behavior_type, user_geohash, item_category, date = parse_line(line)

        # 如果前一个物品pre_category_id和读取到的item_category不一样则输出当前category_features并置空
        if not item_category == pre_category_id:
            other_features = get_other_basic_category_features(tmp_features)  # 获取用户其他特征
            all_features   = merge_features(tmp_features,other_features)
            categorys_features_file.write(pre_category_id + "," + get_category_features_str(all_features) + "\n")  # 输出当前category_features
            initial_tmp_features(tmp_features)  # 初始化置空category_features

        ##计算当前category基本特征
        #行为基本特征计算
        xxx_counts = basic_feature_behaviour[behavior_type][0]
        xxx_user  = basic_feature_behaviour[behavior_type][1]
        user_xxx_first_time  = basic_feature_behaviour[behavior_type][2]
        eachday_xxx_counts  = basic_feature_behaviour[behavior_type][3]

        tmp_features[xxx_counts] += 1
        tmp_features[xxx_user][user_id] = tmp_features[xxx_user].get(user_id, 0) + 1
        tmp_features[eachday_xxx_counts][date.date()] = tmp_features[eachday_xxx_counts].get(date.date(), 0) + 1
        
        if user_id not in tmp_features[user_xxx_first_time]:
            tmp_features[user_xxx_first_time][user_id] = date.date()

        pre_category_id = item_category

    #最后一个category特征
    other_features = get_other_basic_category_features(tmp_features)
    all_features   = merge_features(tmp_features,other_features)
    # print category_features
    #  输出最后一个category_features到文件
    categorys_features_file.write(pre_category_id + "," + get_category_features_str(all_features) + "\n")

    train_file.close()
    categorys_features_file.close()

    print "extract" + begin_date + "---" + begin_date + "categorys features completed"
    return categorys_feat_file_path

def get_other_basic_category_features(tmp_features):

    other_features = {}
    initial_category_features(other_features)
    total_user =  float(len(set(tmp_features['c_click_user']) | set(tmp_features['c_favor_user'])
                                             | set(tmp_features['c_cart_user']) | set(tmp_features['c_buy_user'])))
    
    other_features['c_total_buy_people'] = float(len(set(tmp_features['c_buy_user'])))
                                             
    for behaviour in ['1','2','3','4']:
        lastday_xxx_counts = other_feature_behaviour[behaviour][0]
        total_xxx_people =  other_feature_behaviour[behaviour][1]
        average_xxx = other_feature_behaviour[behaviour][2]

        xxx_counts = basic_feature_behaviour[behaviour][0]
        xxx_user = basic_feature_behaviour[behaviour][1]
        eachday_xxx_counts = basic_feature_behaviour[behaviour][3]
        
        other_features[lastday_xxx_counts] = tmp_features[eachday_xxx_counts].get(lastday, 0)
        other_features[total_xxx_people] =  float(len(set(tmp_features[xxx_user])))
        other_features[average_xxx] = tmp_features[xxx_counts] / (total_user + 1)

    multiple_buy_user = 0
    for count in tmp_features['c_buy_user'].values():
        if count > 2:
            multiple_buy_user += 1

    multiple_click_user = 0
    once_click_user = 0
    for count in tmp_features['c_click_user'].values():
        if count == 1:
            once_click_user += 1
        elif count > 2:
            multiple_click_user += 1

    other_features['c_buy_per_cart'] = float(tmp_features['c_buy_counts']) / (tmp_features['c_cart_counts'] + 1)
    other_features['c_buy_per_favorite'] = float(tmp_features['c_buy_counts']) / (tmp_features['c_favor_counts'] + 1)
    other_features['c_buy_per_click'] = float(tmp_features['c_buy_counts']) / (tmp_features['c_click_counts'] + 1)

    other_features['c_people_buy_per_cart'] = other_features['c_total_buy_people'] / (other_features['c_total_cart_people'] + 1)
    other_features['c_people_buy_per_favorite'] = other_features['c_total_buy_people'] / (other_features['c_total_favor_people'] + 1)
    other_features['c_people_buy_per_click'] = other_features['c_total_buy_people'] / (other_features['c_total_click_people'] + 1)

    # 比值特征
    other_features['c_comeback_rate'] = float(multiple_buy_user) / (other_features['c_total_buy_people'] + 1)
    other_features['c_jump_rate'] = float(once_click_user) / (total_user + 1)
    other_features['c_active_rate'] = float(multiple_click_user) / (total_user + 1)


    return other_features

def merge_features(tmp_features,other_features):

    category_features = {}
    for behaviour in ['1','2','3','4']:
        for userful_index in basic_useful_feat_index:
            feature_name = basic_feature_behaviour[behaviour][userful_index]
            category_features[feature_name] = tmp_features[feature_name]

    category_features.update(other_features)
    return category_features

def get_category_features_str(category_features):
    category_features_str = ''
    for k, v in category_features.iteritems():
        if (type(v) is not types.ListType) and (type(v) is not types.DictType):
            category_features_str += str(v) + ','
    return category_features_str

# path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))+'\\source'
# print path
# os.chdir(path)
# train_file_path = "2014-12-9-2014-12-18-split-all_filtered_unknownitem_tianchi_mobile_recommend_train_user.csv"
# begin_date = "2014-12-9"
# end_date = "2014-12-18"
# extract_categorys_features(train_file_path,begin_date,end_date)
