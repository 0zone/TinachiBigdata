# coding=utf-8
__author__ = 'jinyu'

import os
from util import *

buy_behaviour_type = '4'
cart_behaviour_type = '3'
favorite_behaviour_type = '2'
click_behaviour_type = '1'
delimiter = ','

users_features = 0
items_features = 0
lastday = 0


#遍历文件时候得到的属性
basic_feature_behaviour = {'4':['buy_counts','eachday_buy_counts'],
                           '3':['cart_counts','eachday_cart_counts'],
                           '2':['favor_counts','eachday_favor_counts'],
                           '1':['click_counts','eachday_click_counts']}

#基础属性可以直接使用的
basic_useful_feat_index = [0]

#对基础属性加工时候得到的属性
other_feature_behaviour = {'4':['buy_per_behavior'],
                           '3':['cart_per_behavior'],
                           '2':['favor_per_behavior'],
                           '1':['click_per_behavior']}

def initial_tmp_features(tmp_features):
    ###基础属性
    for featurenames in basic_feature_behaviour.values():
        tmp_features[featurenames[0]] = 0    #xxx_counts # 销量 # 购物车量 # 收藏量 # 点击量
        tmp_features[featurenames[1]] = []   #eachday_xxx_counts{date:count} # 购物车用户 # 消费用户

    tmp_features['user_id'] = 0
    tmp_features['item_id'] = 0
    tmp_features['behavior_counts'] = 0  #累计访问(无论点击、收藏、购物车、购买)
    tmp_features['behavior_days_list'] = []


def initial_ui_features(other_features):
    ###扩展属性
    for featurenames in  other_feature_behaviour.values():
        other_features[featurenames[0]] = 0    #xxx_per_behavior

    other_features['behavior_counts'] = 0  #累计访问
    other_features['lastday_click_counts'] = 0   #1天前点击数
    other_features['behavior_begin_end_days'] = 0   #第一次访问(点击、收藏、购物车、购买)到最后一次访问的时间间隔
    other_features['behavior_days'] = 0   #累计访问天数
    other_features['save_before_buy'] = 0   #是否在购物车里：加入购物车之后还没有进行购买为1，若有购买行为则为0；若没有购物车行为则为0
    other_features['cart_before_buy'] = 0   #是否在收藏夹里：收藏之后还没有进行购买为1，若有购买行为则为0；没有收藏行为则为0

    other_features['buy_per_buyanything'] = 0   #累计购买/(用户)总购买次数
    other_features['behaviordays_per_activedays'] = 0   #累计访问天数/(用户)活跃天数
    other_features['buydays_per_buyanythingdays'] = 0   #累计购买天数/(用户)购买天数


def extract_ui_features(train_file_path,begin_date,end_date):

    train_file = open(train_file_path)
    filename = "./feature/" + begin_date + "-" + end_date + "-uifeat.csv"
    ui_features_file = open(filename, 'w')

    global users_features
    filename = "./feature/"+begin_date+"-"+end_date+"-userfeat.csv"
    user_features = load_features(filename, user_features, 0)

    global items_features
    filename = "./feature/"+begin_date+"-"+end_date+"-itemfeat.csv"
    item_features = load_features(filename, item_features, 1)

    global lastday
    lastday = datetime.datetime.strptime(end_date, "'%Y-%m-%d'").date()-datetime.timedelta(days=1)

    tmp_features = {}
    initial_tmp_features(tmp_features)

    line = train_file.readline()
    pre_ui_id = line.split(delimiter)[0]+','+line.split(delimiter)[1] # 获取第一行的user_id
    train_file.seek(0)
    for line in train_file:
        user_id, item_id, behavior_type, user_geohash, item_category, date = parse_line(line)
        ui_id = user_id +','+ item_id

        tmp_features['user_id'] = user_id
        tmp_features['item_id'] = item_id

        # 如果当前pre_user_id和读取到的user_id不一样则输出当前user_features并置空
        if not ui_id == pre_ui_id:
            other_features = get_other_basic_ui_features(tmp_features)    # 获取用户其他特征
            all_features   = merge_features(tmp_features,other_features)
            ui_features_file.write(pre_ui_id + "," + get_ui_features_str(all_features) + "\n")  # 输出当前user_features
            initial_tmp_features(tmp_features)    # 初始化置空user_features


        #计算当前item基本特征
        tmp_features['behavior_counts'] += 1	#累计访问次数
        tmp_features['behavior_days_list'].append(date.date())   #累计访问天数


        #行为基本特征计算           
        xxx_counts = basic_feature_behaviour[behavior_type][0]
        eachday_xxx_counts  = basic_feature_behaviour[behavior_type][1]

        tmp_features[xxx_counts] += 1
        tmp_features[eachday_xxx_counts][date.date()] = tmp_features[eachday_xxx_counts].get(date.date(), 0) + 1


        pre_ui_id = ui_id

    other_features = get_other_basic_ui_features(tmp_features)    # 获取用户其他特征
    all_features   = merge_features(tmp_features,other_features)
    print all_features # 输出最后一个user_features到文件并重新初始化user_features
    ui_features_file.write(pre_ui_id + "," + get_ui_features_str(all_features) + "\n")

    ui_features_file.close()
    train_file.close()

def get_other_basic_ui_features(tmp_features):

    other_features = {}
    initial_ui_features(other_features)
    other_features['behavior_counts'] = tmp_features['behavior_counts']  #累计访问

    earlist_behavior_date = date(9999,9,9)#最早访问时间
    latest_behavior_date  = date(1111,1,1)#最晚访问时间
    for behaviour in ['1','2','3','4']:
        xxx_count = basic_feature_behaviour[behaviour][0]
        xxx_per_behavior = other_feature_behaviour[behaviour][0]
        eachday_xxx_counts = basic_feature_behaviour[behaviour][1]

        other_features[xxx_per_behavior] = float(tmp_features[xxx_count])/tmp_features['behavior_counts']

        if min(tmp_features[eachday_xxx_counts].keys()) < earlist_behavior_date:
            earlist_behavior_date = min(tmp_features[eachday_xxx_counts].keys())
        if max(tmp_features[eachday_xxx_counts].keys()) > latest_behavior_date:
            latest_behavior_date = max(tmp_features[eachday_xxx_counts].keys())


    #1天前点击数
    other_features['lastday_click_counts'] = tmp_features['eachday_click_counts'].get(lastday , 0)

    #第一次访问(点击、收藏、购物车、购买)到最后一次访问的时间间隔
    other_features['behavior_begin_end_days'] = getattr(latest_behavior_date -  earlist_behavior_date,'days')

    #累计访问天数
    other_features['behavior_days'] = len(set(tmp_features['eachday_buy_counts'].keys())|set(tmp_features['eachday_cart_counts'].keys())|
    set(tmp_features['eachday_favor_counts'].keys())|set(tmp_features['eachday_click_counts'].keys()))

    #是否在收藏夹里：收藏之后还没有进行购买为1，若有购买行为则为0；没有收藏行为则为0
    if not len(tmp_features['eachday_save_counts']) == 0:
        if not len(tmp_features['eachday_buy_counts']) == 0:
            earlist_save_date = min(tmp_features['eachday_save_counts'].keys())#最早购物车时间
            latest_buy_date  =  max(tmp_features['eachday_buy_counts'].keys())#最晚购买时间
            if earlist_save_date < latest_buy_date:#已经购买了
                other_features['save_before_buy'] = 0
            else:
                other_features['save_before_buy'] = 1
        else:
            other_features['save_before_buy'] = 1	#没有购买行为
    else:
        other_features['save_before_buy'] = 0   #没有收藏行为

    #是否在购物车里：加入购物车之后还没有进行购买为1，若有购买行为则为0；若没有购物车行为则为0
    if not len(tmp_features['eachday_cart_counts']) == 0:
        if not len(tmp_features['eachday_buy_counts']) == 0:
            earlist_cart_date = min(tmp_features['eachday_cart_counts'].keys())#最早购物车时间
            latest_buy_date  =  max(tmp_features['eachday_buy_counts'].keys())#最晚购买时间
            if earlist_cart_date < latest_buy_date:#已经购买了
                other_features['cart_before_buy'] = 0
            else:
                other_features['cart_before_buy'] = 1
        else:
            other_features['cart_before_buy'] = 1   #没有购买行为
    else:
        other_features['cart_before_buy'] = 0  #没有收藏行为
     
    user_id = other_features['user_id']
    other_features['buy_per_buyanything'] = float(tmp_features['buy_counts'])/(users_features[user_id]['buy_counts'] + 1)   #累计购买/(用户)总购买次数
    other_features['behaviordays_per_activedays'] = float(tmp_features['buy_counts'])/(users_features[user_id]['active_days_count'] + 1)   #累计访问天数/(用户)活跃天数
    other_features['buydays_per_buyanythingdays'] = float(tmp_features['buy_counts'])/(users_features[user_id]['buy_days_count'] + 1)   #累计购买天数/(用户)购买天数

    return other_features

def merge_features(tmp_features,other_features):

    item_features = {}
    for behaviour in ['1','2','3','4']:
        for userful_index in basic_useful_feat_index:
            feature_name = basic_feature_behaviour[behaviour][userful_index]
            item_features[feature_name] = tmp_features[feature_name]

    item_features.update(other_features)
    return item_features

def get_ui_features_str(user_features):
    user_features_str = ''
    for k, v in user_features.iteritems():
        if type(v) is not types.ListType:
            user_features_str += str(v) + ','

    return user_features_str


path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))+'\\source'
os.chdir(path)  ## change dir to '~/files'
train_file_path = "train_filtered_unknownitem_tianchi_mobile_recommend_train_user.csv"
extract_ui_features(train_file_path)


