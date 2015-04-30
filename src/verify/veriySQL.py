# coding=utf-8
import MySQLdb
import datetime
import math
import re
import os


#数据库配置
sqlhost = '127.0.0.1'
sqluser = 'root'
sqlpswd = 'root'
sqldb   = 'tianchi'

def veriyfeat4user(user_ids,sqls):#userfeat = {'user_id':[]}
	#提取用户特征
	conn=MySQLdb.connect(host=sqlhost,user=sqluser,passwd=sqlpswd,db=sqldb,charset='utf8',port=3306)
	cur=conn.cursor()
	
	feats = {}
	for user_id in user_ids:
		feats[user_id] = {}
		for (name,sql) in sqls.items():
			strinfo = re.compile('user_id = %s')
			nsql = strinfo.sub('user_id = '+str(user_id),sql)
			#print nsql
			cur.execute(nsql)
			
			results = cur.fetchall()
			feats[user_id][name] = results[0][0]
		#print '\n'+str(user_id)+'-----'
		#print feats[user_id]
			
	return feats	

def veriyfeat4item(item_ids,sqls):#userfeat = {'user_id':[]}
	#提取用户特征
	conn=MySQLdb.connect(host=sqlhost,user=sqluser,passwd=sqlpswd,db=sqldb,charset='utf8',port=3306)
	cur=conn.cursor()
	
	feats = {}
	for item_id in item_ids:
		feats[item_id] = {}
		for (name,sql) in sqls.items():
			strinfo = re.compile('item_id = %s')
			nsql = strinfo.sub('item_id = '+str(item_id),sql)
			#print nsql
			cur.execute(nsql)
			
			results = cur.fetchall()
			feats[item_id][name] = results[0][0]
		#print '\n'+str(item_id)+'-----'
		#print feats[item_id]
			
	return feats		
	
def veriyfeat4category(category_ids,sqls):#itemfeat = {'item_id':[]}
	#提取用户特征
	conn=MySQLdb.connect(host=sqlhost,user=sqluser,passwd=sqlpswd,db=sqldb,charset='utf8',port=3306)
	cur=conn.cursor()
	
	feats = {}
	for category_id in category_ids:
		feats[category_id] = {}
# 		print "sql category_id:",category_id
		for (name,sql) in sqls.items():
			strinfo = re.compile('item_category = %s')
			nsql = strinfo.sub('item_category = '+str(category_id),sql)
# 			print nsql
			cur.execute(nsql)
			
			results = cur.fetchall()
			feats[category_id][name] = results[0][0]
		#print '\n'+str(item_id)+'-----'
		#print feats[item_id]
			
	return feats	
	
def veriyfeat4ui(ui_ids,sqls):#userfeat = {'user_id':[]}
	#提取用户特征
	conn=MySQLdb.connect(host=sqlhost,user=sqluser,passwd=sqlpswd,db=sqldb,charset='utf8',port=3306)
	cur=conn.cursor()
	
	feats = {}
	for ui_id in ui_ids:
		feats[ui_id] = {}
		for (name,sql) in sqls.items():
			user_id,item_id = ui_id.split(',')
			strinfo = re.compile('user_id = %s')
			nsql = strinfo.sub('user_id = '+str(user_id),sql)
			
			strinfo = re.compile('item_id = %s')
			nsql = strinfo.sub('item_id = '+str(item_id),nsql)
			cur.execute(nsql)
			results = cur.fetchall()
			feats[ui_id][name] = results[0][0]
			
	return feats	
		

def veriyfeat4uc(ids,sqls):#uifeat = {'user_id':[]}
	#提取用户特征
	conn=MySQLdb.connect(host=sqlhost,user=sqluser,passwd=sqlpswd,db=sqldb,charset='utf8',port=3306)
	cur=conn.cursor()
	
	feats = {}
	for ui_id in ids:
		feats[ui_id] = {}
		for (name,sql) in sqls.items():
			user_id,category_id = ui_id.split(',')
			strinfo = re.compile('user_id = %s')
			nsql = strinfo.sub('user_id = '+str(user_id),sql)
			
			strinfo = re.compile('item_category = %s')
			nsql = strinfo.sub('item_category = '+str(category_id),nsql)
			cur.execute(nsql)
			results = cur.fetchall()
			feats[ui_id][name] = results[0][0]
			
	return feats
	
sql = {
'userfeat':{
'buy_counts':"select count(*) from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19';",
'cart_counts':"select count(*) from train_user_target_item where user_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19';",
'favor_counts':"select count(*) from train_user_target_item where user_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19';",
'click_counts':"select count(*) from train_user_target_item where user_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19';",
'buy_brands_count':"select count(distinct item_id) from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19';",
'cart_brands_count':"select count(distinct item_id) from train_user_target_item where user_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19';",
'favor_brands_count':"select count(distinct item_id) from train_user_target_item where user_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19';",
'click_brands_count':"select count(distinct item_id) from train_user_target_item where user_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19';",
'buy_categorys_count':"select count(distinct item_category) from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19';",
'cart_categorys_count':"select count(distinct item_category) from train_user_target_item where user_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19';",
'favor_categorys_count':"select count(distinct item_category) from train_user_target_item where user_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19';",
'click_categorys_count':"select count(distinct item_category) from train_user_target_item where user_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19';",
'active_days_count':"select count(distinct date(time)) from train_user_target_item where user_id = %s and time >= '2014-12-09' and time < '2014-12-19';",
'buy_days_count':"select count(distinct date(time)) from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19';",
'buy_per_cart':"select (select count(*) from train_user_target_item where user_id = %s and  behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19') /(select count(*)+1 from train_user_target_item where user_id = %s and  behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19');",
'buy_per_favor':"select (select count(*) from train_user_target_item where user_id = %s and  behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19') /(select count(*)+1 from train_user_target_item where user_id = %s and  behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19');",
'buy_per_click':"select (select count(*) from train_user_target_item where user_id = %s and  behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19') /(select count(*)+1 from train_user_target_item where user_id = %s and  behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19');",
'brand_buy_per_cart':"select (select count(distinct item_id) from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19') /(select count(distinct item_id)+1 from train_user_target_item where user_id = %s and  behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19')",
'brand_buy_per_favor':"select (select count(distinct item_id) from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19') /(select count(distinct item_id)+1 from train_user_target_item where user_id = %s and  behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19')",
'brand_buy_per_click':"select (select count(distinct item_id) from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19') /(select count(distinct item_id)+1 from train_user_target_item where user_id = %s and  behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19')",
'buy_days_count_per_active_days_count':"select (select count(distinct date(time)) from train_user_target_item where behavior_type = 4 and user_id = %s and time >= '2014-12-09' and time < '2014-12-19') /(select count(distinct date(time))+1 from train_user_target_item where user_id = %s and time >= '2014-12-09' and time < '2014-12-19');",
'buy_counts_per_buy_days_count':"select (select count(*) from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19') /(select count(distinct date(time))+1 from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19');",
'active_days_count_per_days':"select (select count(distinct date(time)) from train_user_target_item where user_id = %s and time >= '2014-12-09' and time < '2014-12-19')/10 ;"
},
'uifeat':{
'buy_counts':"select count(*) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19';",
'cart_counts':"select count(*) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19';",
'favor_counts':"select count(*) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19';",
'click_counts':"select count(*) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19';",
'buy_per_behavior':"select (select count(*) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19')/(select count(*)+1 from train_user_target_item where user_id = %s and item_id = %s and time >= '2014-12-09' and time < '2014-12-19')",
'cart_per_behavior':"select (select count(*) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19')/(select count(*)+1 from train_user_target_item where user_id = %s and item_id = %s and time >= '2014-12-09' and time < '2014-12-19')",
'favor_per_behavior':"select (select count(*) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19')/(select count(*)+1 from train_user_target_item where user_id = %s and item_id = %s and time >= '2014-12-09' and time < '2014-12-19')",
'click_per_behavior':"select (select count(*) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19')/(select count(*)+1 from train_user_target_item where user_id = %s and item_id = %s and time >= '2014-12-09' and time < '2014-12-19')",
'behavior_counts':"select count(*) from train_user_target_item where user_id = %s and item_id = %s and time >= '2014-12-09' and time < '2014-12-19';",
'lastday_click_counts':"select count(*) from train_user_target_item where user_id = %s and item_id = %s and date(time) = '2014-12-18' and behavior_type = 1;",
'behavior_begin_end_days':"select datediff(max(date(time)) , min(date(time))) from train_user_target_item where user_id = %s and item_id = %s and time >= '2014-12-09' and time < '2014-12-19'",
'behavior_days':"select count(distinct date(time)) from train_user_target_item where user_id = %s and item_id = %s and time >= '2014-12-09' and time < '2014-12-19';",
'save_before_buy':"select (select min(time) from train_user_target_item where user_id = %s and behavior_type = 2 and item_id = %s and time >= '2014-12-09' and time < '2014-12-19') > (select max(time) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19');",
'cart_before_buy':"select (select min(time) from train_user_target_item where user_id = %s and behavior_type = 3 and item_id = %s and time >= '2014-12-09' and time < '2014-12-19') > (select max(time) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19');",
'buy_per_buyanything':"select (select count(*) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19') /(select count(*)+1 from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19');",
'behaviordays_per_activedays':"select (select count(distinct date(time)) from train_user_target_item where user_id = %s and item_id = %s and time >= '2014-12-09' and time < '2014-12-19') /(select count(distinct date(time))+1 from train_user_target_item where user_id = %s and time >= '2014-12-09' and time < '2014-12-19');",
'buydays_per_buyanythingdays':"select (select count(distinct date(time)) from train_user_target_item where user_id = %s and item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19') /(select count(distinct date(time))+1 from train_user_target_item where user_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19');"
},
'itemfeat':{
'sold_counts':"select count(*) from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19';",
'cart_counts':"select count(*) from train_user_target_item where item_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19';",
'favor_counts':"select count(*) from train_user_target_item where item_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19';",
'click_counts':"select count(*) from train_user_target_item where item_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19';",
'lastday_sold_counts':"select count(*) from train_user_target_item where item_id = %s and date(time) = '2014-12-18' and behavior_type = 4;",
'lastday_cart_counts':"select count(*) from train_user_target_item where item_id = %s and date(time) = '2014-12-18' and behavior_type = 3;",
'lastday_favor_counts':"select count(*) from train_user_target_item where item_id = %s and date(time) = '2014-12-18' and behavior_type = 2;",
'lastday_click_counts':"select count(*) from train_user_target_item where item_id = %s and date(time) = '2014-12-18' and behavior_type = 1;",
'total_sold_people':"select count(distinct user_id) from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19';",
'total_cart_people':"select count(distinct user_id) from train_user_target_item where item_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19';",
'total_favor_people':"select count(distinct user_id) from train_user_target_item where item_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19';",
'total_click_people':"select count(distinct user_id) from train_user_target_item where item_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19';",
'average_sold':"select (select count(*) from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19')/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and time >= '2014-12-09' and time < '2014-12-19');",
'average_cart':"select (select count(*) from train_user_target_item where item_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19')/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and time >= '2014-12-09' and time < '2014-12-19');",
'average_favor':"select (select count(*) from train_user_target_item where item_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19')/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and time >= '2014-12-09' and time < '2014-12-19');",
'average_click':"select (select count(*) from train_user_target_item where item_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19')/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and time >= '2014-12-09' and time < '2014-12-19');",
'sold_per_cart':"select (select count(*) from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19')/(select count(*)+1 from train_user_target_item where item_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19')",
'sold_per_favorite':"select (select count(*) from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19')/(select count(*)+1 from train_user_target_item where item_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19')",
'sold_per_click':"select (select count(*) from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19')/(select count(*)+1 from train_user_target_item where item_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19')",
'people_buy_per_cart':"select (select count(distinct user_id) from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19')/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and behavior_type = 3 and time >= '2014-12-09' and time < '2014-12-19')",
'people_buy_per_favorite':"select (select count(distinct user_id) from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19')/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and behavior_type = 2 and time >= '2014-12-09' and time < '2014-12-19')",
'people_buy_per_click':"select (select count(distinct user_id) from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19')/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19')",
'comeback_rate':"select (select count(*) from (select user_id from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19' group by user_id having count(*) > 2) as multiple_buy_user)/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and behavior_type = 4 and time >= '2014-12-09' and time < '2014-12-19')",
'jump_rate':"select (select count(*) from (select user_id from train_user_target_item where item_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19' group by user_id having count(*) = 1) as once_click_user)/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and time >= '2014-12-09' and time < '2014-12-19')",
'active_rate':"select (select count(*) from (select user_id from train_user_target_item where item_id = %s and behavior_type = 1 and time >= '2014-12-09' and time < '2014-12-19' group by user_id having count(*) > 2) as multiple_buy_user)/(select count(distinct user_id)+1 from train_user_target_item where item_id = %s and time >= '2014-12-09' and time < '2014-12-19')"
},
'categoryfeat':{
'c_buy_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17';",
'c_cart_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 3 and time < '2014-12-17';",
'c_favor_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 2 and time < '2014-12-17';",
'c_click_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 1 and time < '2014-12-17';",
'c_lastday_buy_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and time like '2014-12-16%' and behavior_type = 4;",
'c_lastday_cart_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and time like '2014-12-16%' and behavior_type = 3;",
'c_lastday_favor_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and time like '2014-12-16%' and behavior_type = 2;",
'c_lastday_click_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and time like '2014-12-16%' and behavior_type = 1;",
'c_total_buy_people':"select count(distinct user_id) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17';",
'c_total_cart_people':"select count(distinct user_id) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 3 and time < '2014-12-17';",
'c_total_favor_people':"select count(distinct user_id) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 2 and time < '2014-12-17';",
'c_total_click_people':"select count(distinct user_id) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 1 and time < '2014-12-17';",
'c_average_buy':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17')/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and time < '2014-12-17');",
'c_average_cart':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 3 and time < '2014-12-17')/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and time < '2014-12-17');",
'c_average_favor':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 2 and time < '2014-12-17')/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and time < '2014-12-17');",
'c_average_click':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 1 and time < '2014-12-17')/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and time < '2014-12-17');",
'c_buy_per_cart':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17')/(select count(*)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 3 and time < '2014-12-17')",
'c_buy_per_favorite':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17')/(select count(*)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 2 and time < '2014-12-17')",
'c_buy_per_click':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17')/(select count(*)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 1 and time < '2014-12-17')",
'c_people_buy_per_cart':"select (select count(distinct user_id) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17')/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 3 and time < '2014-12-17')",
'c_people_buy_per_favorite':"select (select count(distinct user_id) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17')/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 2 and time < '2014-12-17')",
'c_people_buy_per_click':"select (select count(distinct user_id) from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17')/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 1 and time < '2014-12-17')",
'c_comeback_rate':"select (select count(*) from (select user_id from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17' group by user_id having count(*) > 2) as multiple_buy_user)/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 4 and time < '2014-12-17')",
'c_jump_rate':"select (select count(*) from (select user_id from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 1 and time < '2014-12-17' group by user_id having count(*) = 1) as once_click_user)/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and time < '2014-12-17')",
'c_active_rate':"select (select count(*) from (select user_id from subset_tianchi_mobile_recommend_train_user where item_category = %s and behavior_type = 1 and time < '2014-12-17' group by user_id having count(*) > 2) as multiple_click_user)/(select count(distinct user_id)+1 from subset_tianchi_mobile_recommend_train_user where item_category = %s and time < '2014-12-17')"
},
'ucfeat':{
'uc_buy_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 4 and time < '2014-12-17';",
'uc_cart_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 3 and time < '2014-12-17';",
'uc_favor_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 2 and time < '2014-12-17';",
'uc_click_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 1 and time < '2014-12-17';",
'uc_buy_per_behavior':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 4 and time < '2014-12-17')/(select count(*)+1 from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time < '2014-12-17')",
'uc_cart_per_behavior':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 3 and time < '2014-12-17')/(select count(*)+1 from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time < '2014-12-17')",
'uc_favor_per_behavior':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 2 and time < '2014-12-17')/(select count(*)+1 from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time < '2014-12-17')",
'uc_click_per_behavior':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 1 and time < '2014-12-17')/(select count(*)+1 from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time < '2014-12-17')",
'uc_behavior_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time < '2014-12-17';",
'uc_lastday_click_counts':"select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time like '2014-12-16%' and behavior_type = 1;",
'uc_behavior_begin_end_days':"select (select max(date(time)) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time < '2014-12-17') - (select min(date(time)) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time < '2014-12-17');",
'uc_behavior_days':"select count(distinct date(time)) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time < '2014-12-17';",
'uc_save_before_buy':"select (select min(time) from subset_tianchi_mobile_recommend_train_user where user_id = %s and behavior_type = 2 and item_category = %s and time < '2014-12-17') < (select max(time) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 4 and time < '2014-12-17');",
'uc_cart_before_buy':"select (select min(time) from subset_tianchi_mobile_recommend_train_user where user_id = %s and behavior_type = 3 and item_category = %s and time < '2014-12-17') < (select max(time) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 4 and time < '2014-12-17');",
'uc_buy_per_buyanything':"select (select count(*) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 4 and time < '2014-12-17') /(select count(*)+1 from subset_tianchi_mobile_recommend_train_user where user_id = %s and behavior_type = 4 and time < '2014-12-17');",
'uc_behaviordays_per_activedays':"select (select count(distinct date(time)) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and time < '2014-12-17') /(select count(distinct date(time))+1 from subset_tianchi_mobile_recommend_train_user where user_id = %s and time < '2014-12-17');",
'uc_buydays_per_buyanythingdays':"select (select count(distinct date(time)) from subset_tianchi_mobile_recommend_train_user where user_id = %s and item_category = %s and behavior_type = 4 and time < '2014-12-17') /(select count(distinct date(time))+1 from subset_tianchi_mobile_recommend_train_user where user_id = %s and behavior_type = 4 and time < '2014-12-17');"
}
}

def formatids(filename,idnum):
    input = open(filename)
    output = list()
    if idnum == 1:
        for line in input.readlines():
            output.append(line.strip())
    elif idnum == 2:
        for line in input.readlines():
            entrys = line.strip().split('\t')
            str = entrys[0] + ',' + entrys[1]
            output.append(str) 
    
    return output  
	
def seek1feat(feats,ids,name):
	for id in ids:
		if name not in feats[id]:
			print 'not in'
			return 
		print name+' '+str(id)+':'+str(feats[id][name])
		
		
#veriyfeat4ui(['19095,287045289'],sql['uifeat'])


ui_ids = formatids('./ids.txt',1)
feats = veriyfeat4item(ui_ids,sql['itemfeat'])
seek1feat(feats,ui_ids,'total_sold_people')
#   

#ui_ids = formatids (filename,2)
#ui_ids = ['100014756,102222980','100014756,117800841','100014756,119371605','100014756,147714136','100014756,169652183','100014756,194015732','100014756,233502218','100014756,235910202','100014756,238861461','100014756,314937744','100014756,334822602','100014756,350143717','100014756,403707244','100014756,78608944','100014756,90002384','100014756,94952797','100022513,135893574','100031405,309984252','100058635,124810989','100058635,155580935','100058635,178446166','100058635,298422871','100094762,109214086','100094762,143527392','100094762,169906652']
#veriyfeat4item(['287045289'],sql['itemfeat'])



# conn=MySQLdb.connect(host=sqlhost,user=sqluser,passwd=sqlpswd,db=sqldb,charset='utf8',port=3306)
# cur=conn.cursor()
# sql = 'select (select min(time) from train_user_target_item where user_id = 19095 and behavior_type = 2 and item_id = 287045289) < (select max(time) from train_user_target_item where user_id =  19095 and item_id = 287045289 and behavior_type = 4);'		

# cur.execute(sql)
# results = cur.fetchall()
# if results[0][0] == None:
	# print 'None'
	



#