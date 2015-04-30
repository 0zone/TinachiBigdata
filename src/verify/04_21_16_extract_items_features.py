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
import MySQLdb
import numpy as np
import types

buy_behaviour_type = '4'
cart_behaviour_type = '3'
favor_behaviour_type = '2'
click_behaviour_type = '1'
delimiter = ','
lastdate = 0

#遍历文件时候得到的属性
basic_feature_behaviour = {'4':['sold_counts','sold_user','user_first_sold_time','eachday_sold_counts'],
                           '3':['cart_counts','cart_user','user_first_cart_time','eachday_cart_counts'],
                           '2':['favor_counts','favor_user','user_first_favor_time','eachday_favor_counts'],
                           '1':['click_counts','click_user','user_first_click_time','eachday_click_counts']}



#对基础属性加工时候得到的属性
other_feature_behaviour = {'4':['recent_sold_counts','total_sold_people','average_sold'],
                           '3':['recent_cart_counts','total_cart_people','average_cart'],
                           '2':['recent_favor_counts','total_favor_people','average_favor'],
                           '1':['recent_click_counts','total_click_people','average_click']}

#基础属性可以直接使用的
item_feat_name = []

#基础属性可以直接使用的
useful_feat_name = []

def initial_tmp_features(tmp_features):

    ###基础属性
    for featurename in  basic_feature_behaviour.values():
        tmp_features[featurename[0]] = 0    #xxx_counts # 销量 # 购物车量 # 收藏量 # 点击量
        tmp_features[featurename[1]] = {}   #xxx_user {user:count} # 用户xxx的次数
        tmp_features[featurename[2]] = {}   #user_first_xxx_time {user:first_time}  #用户第一次xxx的时间
        tmp_features[featurename[3]] = {}   #eachday_xxx_counts {time:count}  #每天xxx的数量
       
    #new added
    tmp_features['buy_week_list'] = []#一周购买量分布
    tmp_features['behavior_days_list'] = []#每天销售量、收藏量
    tmp_features['hots_days_list'] = []#每天销售量、收藏量
    tmp_features['multibuy_time_list'] = []#多次购买时间间隔均值
    tmp_features['save_to_buy_time'] = []#收藏到购买时间间隔均值
    tmp_features['cart_to_buy_time'] = []#收藏到购买时间间隔均值

        
def initial_item_features(other_features):

    ###扩展属性
    for featurename in  other_feature_behaviour.values():
        other_features[featurename[0]] = 0    #lastday_xxx_counts #最后一天行为次数
        other_features[featurename[1]] = 0    #total_xxx_people #总购物车人数 
        other_features[featurename[2]] = 0    #average_xxx  #平均xxx量


    other_features['sold_per_cart'] = 0 # 销量/购物量
    other_features['sold_per_favorite'] = 0 # 销量/收藏量
    other_features['sold_per_click'] = 0    # 销量/点击量
    other_features['people_buy_per_cart'] = 0   # 购买人数/购物车人数
    other_features['people_buy_per_favorite'] = 0   #购买人数/收藏人数
    other_features['people_buy_per_click'] = 0  #购买人数/点击人数

    #new added
    other_features['recent_hots'] = []  #最近热度
    other_features['today_buy_prob'] = 0 #今天购买该商品的可能性

    other_features['multibuy_time_span_mean'] = 0#多次购买时间间隔均值
    other_features['multibuy_time_span_std'] = 0#多次购买时间间隔方差

    other_features['save_to_buy_time_span_mean'] = 0#收藏到购买时间间隔均值
    other_features['save_to_buy_time_span_std'] = 0#收藏到购买时间间隔方差

    other_features['cart_to_buy_time_span_mean'] = 0#收藏到购买时间间隔均值
    other_features['cart_to_buy_time_span_std'] = 0#收藏到购买时间间隔方差

    # 比值特征
    other_features['comeback_rate'] = 0 #多次购买的用户数/购买人数
    other_features['jump_rate'] = 0 #只点击过1次的人数/总用户数
    other_features['active_rate'] = 0   #3次及以上点击的用户数/总用户数

def extract_items_features(train_file_path,begin_date,end_date):
    # 按商品id排序
    generate_sortedfile(train_file_path, "sorted_by_item-" + train_file_path, 1)

    train_file = open("sorted_by_item-" + train_file_path)
    items_feat_file_path = "./feature/" + begin_date + "-" + end_date + "-itemfeat.csv"
    items_features_file = open(items_feat_file_path, 'w')

    userful_feat_file_path = "./feature/" + begin_date + "-" + end_date + "-itemusefulfeat.csv"
    useful_features_file = open(userful_feat_file_path, 'w')

    tmp_features = {}
    initial_tmp_features(tmp_features)

    # 输出栏位名
    other_features = get_other_basic_item_features(tmp_features)    # 获取用户其他特征
    item_features,userful_features = merge_features(tmp_features, other_features,item_feat_name,useful_feat_name)
    items_features_file.write("item_id" + "," + get_features_key(item_features) + "\n")
    useful_features_file.write("item_id" + "," + get_features_key(userful_features) + "\n")
    initial_tmp_features(tmp_features)

    global lastdate
    lastdate = datetime.strptime(end_date, "%Y-%m-%d").date() - timedelta(days=1)

    pre_item_id = train_file.readline().split(delimiter)[1]  # 获取第一行的item_id
    train_file.seek(0)
    for line in train_file:
        user_id, item_id, behavior_type, user_geohash, item_category, date = parse_line(line)

        # 如果前一个物品pre_item_id和读取到的item_id不一样则输出当前item_features并置空
        if not item_id == pre_item_id:
            other_features = get_other_basic_item_features(tmp_features)  # 获取用户其他特征
            item_features,userful_features = merge_features(tmp_features, other_features,item_feat_name,useful_feat_name)
            items_features_file.write(pre_item_id + "," + get_features_key(item_features) + "\n")
            useful_features_file.write(pre_item_id + "," + get_features_key(userful_features) + "\n")
            initial_tmp_features(tmp_features)  # 初始化置空item_features

        ##计算当前item基本特征
        #行为基本特征计算
        xxx_counts = basic_feature_behaviour[behavior_type][0]
        xxx_user  = basic_feature_behaviour[behavior_type][1]
        user_xxx_first_time  = basic_feature_behaviour[behavior_type][2]
        eachday_xxx_counts  = basic_feature_behaviour[behavior_type][3] #每天销售量、收藏量

        tmp_features[xxx_counts] += 1
        tmp_features[xxx_user][user_id] = tmp_features[xxx_user].get(user_id, 0) + 1
        tmp_features[eachday_xxx_counts][date.date()] = tmp_features[eachday_xxx_counts].get(date.date(), 0) + 1

        #new-added
        if behavior_type == 4:
            week = datetime.datetime(date.date()).strftime("%w")
            tmp_features['buy_week_list'][week] += 1#一周购买量分布

        tmp_features['multibuy_time_list'][user_id].append(date.date()) #多次购买时间间隔均值

        #收藏到购买时间间隔均值
        if behavior_type == 2:
            tmp_features['save_to_buy_time'][user_id]['save_time'].append(date.date())

        if behavior_type == 4:
            tmp_features['save_to_buy_time'][user_id]['buy_time'].append(date.date())

        #收藏到购买时间间隔均值
        if behavior_type == 2:
            tmp_features['cart_to_buy_time'][user_id]['cart_time'].append(date.date())

        if behavior_type == 4:
            tmp_features['cart_to_buy_time'][user_id]['buy_time'].append(date.date())

        
        if user_id not in tmp_features[user_xxx_first_time]:
            tmp_features[user_xxx_first_time][user_id] = date.date()

        pre_item_id = item_id

    #最后一个item特征
    other_features = get_other_basic_item_features(tmp_features)
    item_features,userful_features = merge_features(tmp_features, other_features,item_feat_name,useful_feat_name)
    items_features_file.write(pre_item_id + "," + get_features_key(item_features) + "\n")
    useful_features_file.write(pre_item_id + "," + get_features_key(userful_features) + "\n")

    train_file.close()
    items_features_file.close()

    return items_feat_file_path

def get_other_basic_item_features(tmp_features):

    other_features = {}
    initial_item_features(other_features)
    total_user =  float(len(set(tmp_features['click_user']) | set(tmp_features['favor_user'])
                                             | set(tmp_features['cart_user']) | set(tmp_features['sold_user'])))
    
    other_features['total_sold_people'] = float(len(set(tmp_features['sold_user'])))
                                             
    for behaviour in ['1','2','3','4']:
        recent_xxx_counts = other_feature_behaviour[behaviour][0]
        total_xxx_people =  other_feature_behaviour[behaviour][1]
        average_xxx = other_feature_behaviour[behaviour][2]

        xxx_counts = basic_feature_behaviour[behaviour][0]
        xxx_user = basic_feature_behaviour[behaviour][1]
        eachday_xxx_counts = basic_feature_behaviour[behaviour][3]

        other_features[recent_xxx_counts] = []
        for gap in [1,3,7,14]:
            num = 0
            for i in range(1,gap):
                date = lastdate - timedelta(days=1)
                num += tmp_features[eachday_xxx_counts].get(date, 0)
            other_features[recent_xxx_counts].append(num)

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

    #new added

    for i in range(0,len(other_features['recent_sold_counts'])):
        recent_hot = 4*other_features['recent_sold_counts'][i]+3*other_features['recent_cart_counts'][i]
        +2*other_features['recent_favor_counts'][i]+1*other_features['recent_click_counts'][i]

        other_features['recent_hots'].append(recent_hot)

    sum = 0
    week = (lastdate + timedelta(days=1)).strftime("%w")#预测日
    for i in tmp_features['buy_week_list']:
        sum += tmp_features['buy_week_list'][i]
    if sum != 0:
        other_features['today_buy_prob'] = float(tmp_features['buy_week_list'][week])/sum #今天购买该商品的可能性
    else:
        other_features['today_buy_prob'] = 0 #今天购买该商品的可能性

    #多次购买时间间隔均值
    multibuy_gap_list = []
    for user_id in tmp_features['tmp_features_list']:
        pre_time = -1
        for time in tmp_features['tmp_features_list'][user_id]:
            if pre_time == -1:
                pre_time = time
            else:
                multibuy_gap_list.append((time - pre_time).days)

    multibuy_gap = np.array(multibuy_gap_list)

    if len(multibuy_gap_list) > 0:
        other_features['multibuy_time_span_mean'] = np.mean(multibuy_gap)
        other_features['multibuy_time_span_std'] = np.std(multibuy_gap)
    else:
        other_features['multibuy_time_span_mean'] = 31
        other_features['multibuy_time_span_std'] = 0.01


    #收藏到购买时间间隔均值
    save_to_buy_gap_list = []
    for user_id in tmp_features['save_to_buy_time']:
        save_time_list = other_features['save_to_buy_time'][user_id]['save_time'].sort()
        buy_time_list = other_features['save_to_buy_time'][user_id]['buy_time']

        for buy_time in buy_time_list:
            flag = True
            for i in range(0, len(save_time_list)):
                if save_time_list[i] > buy_time:#第一个大于buy_time的save_time
                    flag = False
                    if i - 1 >= 0:#有save_time<buy_time,则算差值，没有放弃这个buy_time
                        save_to_buy_gap_list.append((buy_time - save_time_list[i-1]).days)
                    break;
            if flag:#没有大于buy_time的save_time,选一个最大的save_time
                save_to_buy_gap_list.append((buy_time - save_time_list[-1]).days)

    save_to_buy_gap = np.array(save_to_buy_gap_list)
    other_features['save_to_buy_time_span_mean'] = np.mean(save_to_buy_gap)#收藏到购买时间间隔均值
    other_features['save_to_buy_time_span_std'] = np.std(save_to_buy_gap)#收藏到购买时间间隔方差

    #购物车到购买时间间隔均值
    cart_to_buy_gap_list = []
    for user_id in tmp_features['cart_to_buy_time']:
        cart_time_list = other_features['cart_to_buy_time'][user_id]['cart_time'].sort()
        buy_time_list = other_features['cart_to_buy_time'][user_id]['buy_time']

        for buy_time in buy_time_list:
            flag = True
            for i in range(0, len(cart_time_list)):
                if cart_time_list[i] > buy_time:#第一个大于buy_time的cart_time
                    flag = False
                    if i - 1 >= 0:#有save_time<buy_time,则算差值，没有放弃这个buy_time
                        cart_to_buy_gap_list.append((buy_time - cart_time_list[i-1]).days)
                    break;
            if flag:#没有大于buy_time的cart_time,选一个最大的save_time
                cart_to_buy_gap_list.append((buy_time - cart_time_list[-1]).days)


    cart_to_buy_gap = np.array(cart_to_buy_gap_list)
    other_features['cart_to_buy_time_span_mean'] = np.mean(cart_to_buy_gap)#收藏到购买时间间隔均值
    other_features['cart_to_buy_time_span_std'] = np.std(cart_to_buy_gap)#收藏到购买时间间隔方差


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
