# coding=utf-8
__author__ = 'jinyu'

import os
from util import *

buy_behaviour_type = '4'
cart_behaviour_type = '3'
favorite_behaviour_type = '2'
click_behaviour_type = '1'
delimiter = ','

users_features = {}
users_column_index = {}
items_features = {}
items_column_index = {}
lastday = 0


#遍历文件时候得到的属性
basic_feature_behaviour = {'4': ['buy_counts', 'eachday_buy_counts'],
                           '3': ['cart_counts', 'eachday_cart_counts'],
                           '2': ['favor_counts', 'eachday_favor_counts'],
                           '1': ['click_counts', 'eachday_click_counts']}

#基础属性可以直接使用的
basic_useful_feat_index = [0]

#对基础属性加工时候得到的属性
other_feature_behaviour = {'4': ['buy_per_behavior'],
                           '3': ['cart_per_behavior'],
                           '2': ['favor_per_behavior'],
                           '1': ['click_per_behavior']}

def initial_tmp_features(tmp_features):
    ###基础属性
    for featurenames in basic_feature_behaviour.values():
        tmp_features[featurenames[0]] = 0    #xxx_counts # 销量 # 购物车量 # 收藏量 # 点击量
        tmp_features[featurenames[1]] = {}   #eachday_xxx_counts{date:count} # 购物车用户 # 消费用户

    tmp_features['user_id'] = 0
    tmp_features['item_id'] = 0
    tmp_features['behavior_counts'] = 0  #累计访问(无论点击、收藏、购物车、购买)
    tmp_features['behavior_days_list'] = []
    tmp_features['earlist_favor_time'] = datetime.strptime("2099-12-31 0", "%Y-%m-%d %H")
    tmp_features['last_buy_time'] = datetime.strptime("1990-1-1 0", "%Y-%m-%d %H")
    tmp_features['earlist_cart_time'] = datetime.strptime("2099-12-31 0", "%Y-%m-%d %H")

    tmp_features['lastday_cart_counts'] = 0
    tmp_features['lastday_evening_cart_counts'] = 0

def initial_ui_features(other_features):
    # ##扩展属性
    for featurenames in other_feature_behaviour.values():
        other_features[featurenames[0]] = 0    # xxx_per_behavior

    other_features['behavior_counts'] = 0  # 累计访问
    other_features['lastday_click_counts'] = 0   # 1天前点击数
    other_features['behavior_begin_end_days'] = 0   # 第一次访问(点击、收藏、购物车、购买)到最后一次访问的时间间隔
    other_features['behavior_days'] = 0   # 累计访问天数
    other_features['save_before_buy'] = 0   # 是否在购物车里：收藏之后还没有进行购买为1，若有购买行为则为0；若没有收藏行为则为0
    other_features['cart_before_buy'] = 0   # 是否在收藏夹里：加入购物车之后还没有进行购买为1，若有购买行为则为0；没有购物车行为则为0

    other_features['buy_per_buyanything'] = 0   #累计购买/(用户)总购买次数
    other_features['behaviordays_per_activedays'] = 0   #累计访问天数/(用户)活跃天数
    other_features['buydays_per_buyanythingdays'] = 0   #累计购买天数/(用户)购买天数


def extract_ui_features(train_file_path, begin_date, end_date):
    print "\n" + begin_date + "---" + end_date + " extracting ui features..."

    generate_sortedfile(train_file_path, "temp/sorted_by_ui-" + train_file_path.split('/')[-1], 0, 1)
    train_file = open("temp/sorted_by_ui-" + train_file_path.split('/')[-1])

    ui_feat_file_path = "./feature/" + begin_date + "-" + end_date + "-uifeat.csv"
    ui_features_file = open(ui_feat_file_path, 'w')

    global users_features, users_column_index
    filename = "./feature/"+begin_date+"-"+end_date+"-userfeat.csv"
    users_column_index, users_features = load_features(filename)

    # global items_features, items_column_index
    # filename = "./feature/"+begin_date+"-"+end_date+"-itemfeat.csv"
    # items_column_index, items_features = load_features(filename)

    global lastday
    lastday = datetime.strptime(end_date, "%Y-%m-%d").date() - timedelta(days=1)

    lastday_18 = datetime.strptime(str(lastday) + " 18:0:0", "%Y-%m-%d %H:%M:%S")
    lastday_22 = datetime.strptime(str(lastday) + " 22:0:0", "%Y-%m-%d %H:%M:%S")

    tmp_features = {}
    initial_tmp_features(tmp_features)

    other_features = get_other_basic_ui_features(tmp_features)    # 获取用户其他特征
    all_features = merge_features(tmp_features, other_features)
    # print get_features_key(all_features)
    ui_features_file.write("user_id" + "," + "item_id" + "," + get_features_key(all_features) + "\n")  # 输出当前ui_features标题

    initial_tmp_features(tmp_features)    # 初始化置空ui_features

    line = train_file.readline()
    pre_ui_id = line.split(delimiter)[0] + ',' + line.split(delimiter)[1]   # 获取第一行的user_id,item_id
    train_file.seek(0)
    for line in train_file:
        user_id, item_id, behavior_type, user_geohash, item_category, date = parse_line(line)
        ui_id = user_id + ',' + item_id

        tmp_features['user_id'] = user_id
        tmp_features['item_id'] = item_id

        # 如果当前pre_ui_id和读取到的ui_id不一样则输出当前ui_features并置空
        if not ui_id == pre_ui_id:
            other_features = get_other_basic_ui_features(tmp_features)    # 获取ui其他特征
            all_features = merge_features(tmp_features, other_features)
            if all_features["cart_counts"] > 0:    # windows天从未加过购物车的筛除
                ui_features_file.write(pre_ui_id + "," + get_ui_features_str(all_features) + "\n")  # 输出当前ui_features

            initial_tmp_features(tmp_features)    # 初始化置空ui_features

        if behavior_type == '4' and date > tmp_features['last_buy_time']:
            tmp_features['last_buy_time'] = date
        
        if behavior_type == '3' and date < tmp_features['earlist_cart_time']:
            tmp_features['earlist_cart_time'] = date
        
        if behavior_type == '2' and date < tmp_features['earlist_favor_time']:
            tmp_features['earlist_favor_time'] = date

        tmp_features['behavior_counts'] += 1    # 累计访问次数
        tmp_features['behavior_days_list'].append(date.date())   # 访问天

        # 行为基本特征计算
        xxx_counts = basic_feature_behaviour[behavior_type][0]
        eachday_xxx_counts = basic_feature_behaviour[behavior_type][1]

        tmp_features[xxx_counts] += 1
        tmp_features[eachday_xxx_counts][date.date()] = tmp_features[eachday_xxx_counts].get(date.date(), 0) + 1

        if (behavior_type == cart_behaviour_type) and (date >= lastday_18) and (date <= lastday_22):
            tmp_features['lastday_evening_cart_counts'] += 1
        if (behavior_type == cart_behaviour_type) and (date.date() == lastday):
            tmp_features['lastday_cart_counts'] += 1

        pre_ui_id = ui_id

    other_features = get_other_basic_ui_features(tmp_features)    # 获取用户其他特征
    all_features = merge_features(tmp_features, other_features)
    if all_features["cart_counts"] > 0:    # windows天从未加过购物车的筛除
        ui_features_file.write(pre_ui_id + "," + get_ui_features_str(all_features) + "\n")  # 输出当前ui_features

    ui_features_file.close()
    train_file.close()

    print "extract" + begin_date + "---" + begin_date + "ui features completed"
    return ui_feat_file_path


def get_other_basic_ui_features(tmp_features):

    other_features = {}
    initial_ui_features(other_features)
    other_features['behavior_counts'] = tmp_features['behavior_counts']  #累计访问

    earlist_behavior_date = date(2099, 12, 31)  # 最早访问时间
    latest_behavior_date = date(1990, 1, 1)     # 最晚访问时间

    for behaviour in ['1', '2', '3', '4']:
        xxx_count = basic_feature_behaviour[behaviour][0]
        xxx_per_behavior = other_feature_behaviour[behaviour][0]
        eachday_xxx_counts = basic_feature_behaviour[behaviour][1]

        other_features[xxx_per_behavior] = float(tmp_features[xxx_count]) / (tmp_features['behavior_counts'] + 1)

    # 如果不空时，计算earlist_behavior_date，latest_behavior_date
    if tmp_features['behavior_days_list']:
        earlist_behavior_date = min(tmp_features['behavior_days_list'])
        latest_behavior_date = max(tmp_features['behavior_days_list'])

    # 最后一天点击数
    other_features['lastday_click_counts'] = tmp_features['eachday_click_counts'].get(lastday, 0)

    # 第一次访问(点击、收藏、购物车、购买)到最后一次访问的时间间隔
    other_features['behavior_begin_end_days'] = (latest_behavior_date - earlist_behavior_date).days

    # 累计访问天数
    other_features['behavior_days'] = len(set(tmp_features['eachday_buy_counts'].keys()) | set(tmp_features['eachday_cart_counts'].keys()) |
                                          set(tmp_features['eachday_favor_counts'].keys()) | set(tmp_features['eachday_click_counts'].keys()))

    # 是否在收藏夹里：收藏之后还没有进行购买为1，若有购买行为则为0；没有收藏行为则为0
    if tmp_features['earlist_favor_time'] == datetime.strptime("2099-12-31 0", "%Y-%m-%d %H"):  # 没有收藏行为
        other_features['save_before_buy'] = 0
    else:
        if tmp_features['last_buy_time'] == datetime.strptime("1990-1-1 0", "%Y-%m-%d %H"):     # 有收藏但没有购买行为
            other_features['save_before_buy'] = 1
        else:   # 有收藏有购买行为
            if tmp_features['last_buy_time'] > tmp_features['earlist_favor_time']:  # 收藏之后有购买行为
                other_features['save_before_buy'] = 0
            else:
                other_features['save_before_buy'] = 1   # 收藏之后还没有进行购买 或 收藏之前有购买行为

    # 是否在购物车里：加入购物车之后还没有进行购买为1，若有购买行为则为0；若没有购物车行为则为0
    if tmp_features['earlist_cart_time'] == datetime.strptime("2099-12-31 0", "%Y-%m-%d %H"):   # 没有购物车行为
        other_features['cart_before_buy'] = 0
    else:
        if tmp_features['last_buy_time'] == datetime.strptime("1990-1-1 0", "%Y-%m-%d %H"):     # 有购物车但没有购买行为
            other_features['cart_before_buy'] = 1
        else:   # 有购物车有购买行为
            if tmp_features['last_buy_time'] > tmp_features['earlist_cart_time']:   # 购物车之后有购买行为
                other_features['cart_before_buy'] = 0
            else:
                other_features['cart_before_buy'] = 1   # 购物车之后还没有进行购买 或 购物车之前有购买行为

    user_id = tmp_features['user_id']
    if user_id != 0:
        other_features['buy_per_buyanything'] = float(tmp_features['buy_counts']) / (float(users_features[user_id][users_column_index['buy_counts']]) + 1)   #累计购买/(用户)总购买次数
        other_features['behaviordays_per_activedays'] = float(other_features['behavior_days']) / (float(users_features[user_id][users_column_index['active_days_count']]) + 1)   #累计访问天数/(用户)活跃天数
        other_features['buydays_per_buyanythingdays'] = float(len(tmp_features['eachday_buy_counts'].keys())) / (float(users_features[user_id][users_column_index['buy_days_count']]) + 1)   #累计购买天数/(用户)购买天数

    other_features['lastday_cart_counts'] = tmp_features['lastday_cart_counts']
    other_features['lastday_evening_cart_counts'] = tmp_features['lastday_evening_cart_counts']
    return other_features


def merge_features(tmp_features, other_features):

    ui_features = {}
    for behaviour in ['1', '2', '3', '4']:
        for userful_index in basic_useful_feat_index:
            feature_name = basic_feature_behaviour[behaviour][userful_index]
            ui_features[feature_name] = tmp_features[feature_name]

    ui_features.update(other_features)
    return ui_features


def get_ui_features_str(ui_features):
    ui_features_str = ''
    for k, v in ui_features.iteritems():
        if (type(v) is not types.ListType) and (type(v) is not types.DictType):
            ui_features_str += str(v) + ','

    return ui_features_str


# path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))+'\\source'
# os.chdir(path)  ## change dir to '~/files'
# # train_file_path = "validation_filtered_unknownitem_tianchi_mobile_recommend_train_user.csv"
# train_file_path = "2014-12-9-2014-12-18-split-all_filtered_unknownitem_tianchi_mobile_recommend_train_user.csv"
# extract_ui_features(train_file_path, "2014-12-9", "2014-12-18")


