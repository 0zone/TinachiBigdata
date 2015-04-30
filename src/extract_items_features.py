#!/usr/bin/python
# -*- coding: utf-8 -*-

# brand
# sold,sold_user{user,count}
# cart,cart_user[]
# fav,fav_user[]
# click,click_user[]

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
basic_feature_behaviour = {'4':['sold_counts','sold_user','user_first_sold_time','eachday_sold_counts'],
                           '3':['cart_counts','cart_user','user_first_cart_time','eachday_cart_counts'],
                           '2':['favor_counts','favor_user','user_first_favor_time','eachday_favor_counts'],
                           '1':['click_counts','click_user','user_first_click_time','eachday_click_counts']}

#基础属性可以直接使用的
basic_useful_feat_index = [0]

# 需要输出的item特征名    userfeat.csv
user_feat_name = ["jump_rate", "sold_per_favorite", "lastday_cart_counts", "average_sold", "active_rate", "average_cart", "lastday_favor_counts", "total_favor_people", "comeback_rate", "average_favor", "lastday_click_counts", "people_buy_per_click", "cart_counts", "total_cart_people", "sold_counts", "average_click", "people_buy_per_favorite", "total_click_people", "sold_per_click", "lastday_sold_counts", "sold_per_cart", "click_counts", "total_sold_people", "people_buy_per_cart", "favor_counts"]
# 需要输出的user_all特征       userfeat_all.csv
useful_feat_name = ["jump_rate", "sold_per_favorite", "lastday_cart_counts", "average_sold"]


#对基础属性加工时候得到的属性
other_feature_behaviour = {'4':['lastday_sold_counts','total_sold_people','average_sold'],
                           '3':['lastday_cart_counts','total_cart_people','average_cart'],
                           '2':['lastday_favor_counts','total_favor_people','average_favor'],
                           '1':['lastday_click_counts','total_click_people','average_click']}

def initial_tmp_features(tmp_features):

    ###基础属性
    for featurename in  basic_feature_behaviour.values():
        tmp_features[featurename[0]] = 0    #xxx_counts # 销量 # 购物车量 # 收藏量 # 点击量
        tmp_features[featurename[1]] = {}   #xxx_user {user:count} # 用户xxx的次数
        tmp_features[featurename[2]] = {}   #user_first_xxx_time {user:first_time}  #用户第一次xxx的时间
        tmp_features[featurename[3]] = {}   #eachday_xxx_counts {time:count}  #每天xxx的数量
       

        
def initial_item_features(other_features):

    ###扩展属性
    for featurename in  other_feature_behaviour.values():
        other_features[featurename[0]] = 0    #lastday_xxx_counts #最后一天行为次数
        other_features[featurename[1]] = 0    #total_xxx_people #总购物车人数 
        other_features[featurename[2]] = 0    #average_xxx  #平均xxx量
 
    #item_features['careful_user'] = 0.0         # 不在初次访问品牌时购买的订单数 ???????


    other_features['sold_per_cart'] = 0
    other_features['sold_per_favorite'] = 0
    other_features['sold_per_click'] = 0
    other_features['people_buy_per_cart'] = 0
    other_features['people_buy_per_favorite'] = 0
    other_features['people_buy_per_click'] = 0

    # 比值特征
    other_features['comeback_rate'] = 0
    other_features['jump_rate'] = 0
    other_features['active_rate'] = 0

def extract_items_features(train_file_path,begin_date,end_date):
    # 按商品id排序
    print "\n" + begin_date + "---" + end_date + "extracting items features..."
    generate_sortedfile(train_file_path, "temp/sorted_by_item-" + train_file_path.split('/')[-1], 1)

    train_file = open("temp/sorted_by_item-" + train_file_path.split('/')[-1])
    items_feat_file_path = "./feature/" + begin_date + "-" + end_date + "-itemfeat.csv"
    items_features_file = open(items_feat_file_path, 'w')

    tmp_features = {}
    initial_tmp_features(tmp_features)

    # 输出栏位名
    other_features = get_other_basic_item_features(tmp_features)    # 获取用户其他特征
    all_features = merge_features(tmp_features, other_features)
    # print get_features_key(all_features)
    items_features_file.write("item_id" + "," + get_features_key(all_features) + "\n")
    initial_tmp_features(tmp_features)

    global lastday
    lastday = datetime.strptime(end_date, "%Y-%m-%d").date() - timedelta(days=1)
    
    pre_item_id = train_file.readline().split(delimiter)[1]  # 获取第一行的item_id
    train_file.seek(0)
    for line in train_file:
        user_id, item_id, behavior_type, user_geohash, item_category, date = parse_line(line)

        # 如果前一个物品pre_item_id和读取到的item_id不一样则输出当前item_features并置空
        if not item_id == pre_item_id:
            other_features = get_other_basic_item_features(tmp_features)  # 获取用户其他特征
            all_features   = merge_features(tmp_features,other_features)
            items_features_file.write(pre_item_id + "," + get_item_features_str(all_features) + "\n")  # 输出当前item_features
            initial_tmp_features(tmp_features)  # 初始化置空item_features

        ##计算当前item基本特征
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

        pre_item_id = item_id

    #最后一个item特征
    other_features = get_other_basic_item_features(tmp_features)
    all_features   = merge_features(tmp_features,other_features)
    # print item_features
    #  输出最后一个item_features到文件
    items_features_file.write(pre_item_id + "," + get_item_features_str(all_features) + "\n")

    train_file.close()
    items_features_file.close()

    print "extract" + begin_date + "---" + begin_date + "items features completed"
    return items_feat_file_path


def get_other_basic_item_features(tmp_features):

    other_features = {}
    initial_item_features(other_features)
    total_user =  float(len(set(tmp_features['click_user']) | set(tmp_features['favor_user'])
                                             | set(tmp_features['cart_user']) | set(tmp_features['sold_user'])))
    
    other_features['total_sold_people'] = float(len(set(tmp_features['sold_user'])))
                                             
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
    for count in tmp_features['sold_user'].values():
        if count > 2:
            multiple_buy_user += 1

    multiple_click_user = 0
    once_click_user = 0
    for count in tmp_features['click_user'].values():
        if count == 1:
            once_click_user += 1
        elif count > 2:
            multiple_click_user += 1

    other_features['sold_per_cart'] = float(tmp_features['sold_counts']) / (tmp_features['cart_counts'] + 1)
    other_features['sold_per_favorite'] = float(tmp_features['sold_counts']) / (tmp_features['favor_counts'] + 1)
    other_features['sold_per_click'] = float(tmp_features['sold_counts']) / (tmp_features['click_counts'] + 1)

    other_features['people_buy_per_cart'] = other_features['total_sold_people'] / (other_features['total_cart_people'] + 1)
    other_features['people_buy_per_favorite'] = other_features['total_sold_people'] / (other_features['total_favor_people'] + 1)
    other_features['people_buy_per_click'] = other_features['total_sold_people'] / (other_features['total_click_people'] + 1)

    # 比值特征
    other_features['comeback_rate'] = float(multiple_buy_user) / (other_features['total_sold_people'] + 1)
    other_features['jump_rate'] = float(once_click_user) / (total_user + 1)
    other_features['active_rate'] = float(multiple_click_user) / (total_user + 1)


    return other_features

def merge_features(tmp_features,other_features):

    item_features = {}
    for behaviour in ['1','2','3','4']:
        for userful_index in basic_useful_feat_index:
            feature_name = basic_feature_behaviour[behaviour][userful_index]
            item_features[feature_name] = tmp_features[feature_name]

    item_features.update(other_features)
    return item_features

def get_item_features_str(item_features):
    item_features_str = ''
    for k, v in item_features.iteritems():
        if (type(v) is not types.ListType) and (type(v) is not types.DictType):
            item_features_str += str(v) + ','
    return item_features_str

# path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))+'\\source'
# print path
# os.chdir(path)
# train_file_path = "2014-12-9-2014-12-18-split-all_filtered_unknownitem_tianchi_mobile_recommend_train_user.csv"
# begin_date = "2014-12-9"
# end_date = "2014-12-18"
# extract_items_features(train_file_path,begin_date,end_date)
