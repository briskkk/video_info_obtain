# -*- coding:UTF-8 -*-
 
# 获取单机游戏的视频列表，tid=17
# 将全部信息获取到video_info_all中，再写入数据库

import os
import json
import requests
import platform
from time import *
from multiprocessing.dummy import Pool as ThreadPool
from classes import *
import pymysql

video_info_all = []

# 先访问page1，然后可以获取到一共有多少个视频，按照每页放50个（经测试最多可以放50个，数量继续增加无效），做除法得到共分为多少页，再对每页进行遍历，获取每个视频的aid好以及标签，之后根据aid的URL获取每个aid的具体点赞量等信息。
# get_page_num -- 获取视频排行总页数（每页设置为50个）
def get_page_num(URL_zone,page=1):
	VIDEOS_PER_PAGE = 50
	URL_PAGE1 = URL_zone + "&pn={0}&ps=20".format(page)
	VID_INFO1 = requests.get(url = URL_PAGE1).json()  # 从第一页获取到的前二十个视频列表
	VID_COUNT = VID_INFO1['data']['page']['count']  # 视频总数
	if VID_COUNT%VIDEOS_PER_PAGE != 0:
		VID_PAGES = VID_COUNT//VIDEOS_PER_PAGE + 1  # 视频页数（最后一页视频数不足50）
	else:
		VID_PAGES = VID_COUNT//VIDEOS_PER_PAGE  # 视频页数
	return VID_PAGES
	
# get_video_info -- 获取某个页码上的视频对象信息，返回每页的视频对象列表
def get_video_info(page_num):
	video_info_list = []
	URL_PAGE = URL_zone + "&pn={0}&ps=50".format(page_num)
	VID_INFO = requests.get(url = URL_PAGE).json()  # 从第page_count页获取视频列表
	sleep(0.5)  # 延迟，避免太快ip被封
	VID_ARCHIVES = VID_INFO['data']['archives']  # 每页中视频列表及每个视频标签等信息
	for video_count in range(len(VID_ARCHIVES)):  # 获取50个视频中每个视频的信息，传到对象Video中
		video = Video()
		video_info = VID_ARCHIVES[video_count]  # 保存第video_count个视频的信息
		video.aid = video_info['aid']  # 视频编号
		video.title = video_info['title']  # 标题
		# video.description = video_info['desc']  # 描述
		video.biaoqian = video_info['dynamic']  # 视频描述标签
		video.view = video_info['stat']['view']  # 播放量
		video.danmu = video_info['stat']['danmaku']  # 弹幕数
		video.reply = video_info['stat']['reply']  # 评论数
		video.shoucang = video_info['stat']['favorite']  # 收藏数
		video.coin = video_info['stat']['coin']  # 硬币数
		video.share = video_info['stat']['share']  # 硬币数
		video.like = video_info['stat']['like']  # 点赞数
		video.dislike = video_info['stat']['dislike']
		video.author_mid = video_info['owner']['mid']
		video.author_name = video_info['owner']['name']
		video_info_list.append(video)
	return video_info_list  # 返回视频对象列表
	
# 已完成视频信息的读取以及多线程的实现，接下来需要把信息存到数据库，周期运行代码功能。
		
def create_video_db():
	# 创建数据库
	global cursor
	cursor.execute("""create table if not exists bili_video
					(v_aid int primary key,
					v_title text,
					v_biaoqian text,
					v_view int,
					v_danmu int,
					v_reply int,
					v_favor int,
					v_coin int,
					v_share int,
					v_like int,
					v_dislike int,
					v_author_mid int,
					v_author_name text)
				""")
				
def save_video_db(video_instance_per_page):
	# 将数据保存至本地
	global cursor,conn
	sql = "insert into bili_video values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
	for video_instance in video_instance_per_page:
		row = [video_instance.aid,
				video_instance.title,
				video_instance.biaoqian,
				video_instance.view,
				video_instance.danmu,
				video_instance.reply,
				video_instance.shoucang,
				video_instance.coin,
				video_instance.share,
				video_instance.like,
				video_instance.dislike,
				video_instance.author_mid,
				video_instance.author_name
				]
		try:
			# 执行sql语句
			cursor.execute(sql,row)
		except:
			# 发生错误时回滚
			conn.rollback()
	# 提交到数据库执行
	conn.commit()


if __name__ == "__main__":
	
	# 将数据存入数据库
	# 建立和数据库系统的连接
	conn = pymysql.connect(host='localhost',port=3306,user='root',password='mysql',database='video_db',charset='utf8')
	# 创建游标
	cursor = conn.cursor()
	# 创建数据库中表格bili_video
	create_video_db()
	
	URL_zone = "https://api.bilibili.com/x/web-interface/newlist?rid=17&type=0"
	num_of_pages = get_page_num(URL_zone)  # 获取视频排行总页数（每页设置为50个）
	print('num of pages: '+str(num_of_pages))
	print("#######################")
	starttime = time()
	# pagelist = range(1,num_of_pages+1)
	pagelist = range(1,9)
	pool = ThreadPool(8)
	ret = pool.map(get_video_info,pagelist)
	video_info_all = video_info_all+ret
	pool.close()
	pool.join()
	# 将所有视频信息传入数据库
	for video_per_page in video_info_all:
		save_video_db(video_per_page)
	# print(results[0]) # 每个视频的信息list
	# parse_video(results)
	endtime = time()
	print('Use time is ',(endtime-starttime))
	print('total pages %s' % str(num_of_pages))
	# for i in range(50):
		# print(video_info_all[0][i].like)
	# video_info_all[i] -- 第i页的所有video对象，video_info_all[i][j] -- 第i页第j个video对象

	# 关闭游标
	cursor.close()
	# 关闭连接
	conn.close()
