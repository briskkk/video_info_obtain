# -*- coding:UTF-8 -*-
 
# 获取单机游戏的视频列表，tid=17

import os
import json
import requests
import platform
from time import *
from multiprocessing.dummy import Pool as ThreadPool
from classes import *

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
		
# def parse_video(video_messages_list):
	# num_of_hot_video = 0
	# number = 1
	# f = open("hot_video.txt" , "w")
	# f.write("***********************************************\n")
	# # for video_id in range(6,100):
		# # video = require_video(video_id)
		# # print(video)
	# for video in video_messages_list[0]:
		# if(video[1] == -1):
			# # print("第" + str(number) + "个视频不存在")
			# number += 1
		# elif(int(video[4]) >= 100000):
			# num_of_hot_video += 1
			# f.write("av号：" + str(video[0]) + "播放量：" + str(video[4]) + "\n")
			# f.write("收藏：" + str(video[1]) + "\n")
			# f.write("弹幕数：" + str(video[2]) + "\n")
			# f.write("硬币数：" + str(video[3]) + "\n")
			# f.write("分享数：" + str(video[5]) + "\n")
			# f.write("评论数：" + str(video[6]) + "\n")
			# f.write("顶：" + str(video[7]) + "\n")
			# f.write("踩：" + str(video[8]) + "\n\n")
			# print("第" + str(number) + "个视频的播放量为：" + str(video[4]))
			# number += 1
		# else:
			# number += 1
	# f.write("************************************************\n")
	# f.close()
	# print("av号从1开始的视频中播放数超过1w的视频有" + str(num_of_hot_video) + "个")


if __name__ == "__main__":
	URL_zone = "https://api.bilibili.com/x/web-interface/newlist?rid=17&type=0"
	num_of_pages = get_page_num(URL_zone)
	print('num of pages: '+str(num_of_pages))
	print("#######################")
	starttime = time()
	# pagelist = range(1,num_of_pages+1)
	pagelist = range(1,9)
	video_info_all = []
	pool = ThreadPool(8)
	ret = pool.map(get_video_info,pagelist)
	video_info_all = video_info_all+ret
	pool.close()
	pool.join()
	# print(results[0]) # 每个视频的信息list
	# parse_video(results)
	endtime = time()
	print('Use time is ',(endtime-starttime))
	for i in range(50):
		print(video_info_all[0][i].like)
	# video_info_all[i] -- 第i页的所有video对象，video_info_all[i][j] -- 第i页第j个video对象
