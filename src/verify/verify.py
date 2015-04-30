# coding=utf-8


import os

__author__ = 'jinyu'


day15 = '2014-12-15'
day16 = '2014-12-16'
day17 = '2014-12-17'
day18 = '2014-12-18'


def verify(verify_file_path):
    verify_file = open(verify_file_path)
    result_file = open("verify_result.csv", 'w')

    last_day_buy_uipairs = set()    # 最后一天购买的用户对
    # 找到18号购买的ui对
    for line in verify_file:
        user_id, item_id, behavior_type, user_geohash, item_category, date = line.split(',')
        ui_pair = user_id + "," + item_id
        if (date.split(' ')[0] == day18) and (behavior_type == '4'):
            last_day_buy_uipairs.add(ui_pair)

    # print last_day_buy_uipairs

    verify_file.seek(0)
    for line in verify_file:
        user_id, item_id, behavior_type, user_geohash, item_category, date = line.split(',')
        if date.split(' ')[0] == day15 or date.split(' ')[0] == day16 or date.split(' ')[0] == day17:
            ui_pair = user_id + "," + item_id
            if ui_pair in last_day_buy_uipairs:
                result_file.write(user_id+","+item_id+"\n")
                # print(user_id+","+item_id)

    verify_file.close()
    result_file.close()

verify("D:\\tianchi_mobile_recommend_train_user.csv")

