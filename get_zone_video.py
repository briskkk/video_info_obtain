# -*- coding:UTF-8 -*-
 
# 获取单机游戏的视频列表，tid=17
# 将全部信息获取到video_info_all中，再写入数据库

import os
import json
import requests
import platform
from time import *
from multiprocessing.dummy import Pool as ThreadPool
from apscheduler.schedulers.background import BackgroundScheduler
from classes import *
import pymysql
from secret import *

video_info_all = []
scheduler = BackgroundScheduler()
URL_zone = "https://api.bilibili.com/x/web-interface/newlist?rid=17&type=0"

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
    global URL_zone
    video_info_list = []
    URL_PAGE = URL_zone + "&pn={0}&ps=50".format(page_num)
    VID_INFO = requests.get(url = URL_PAGE).json()  # 从第page_count页获取视频列表
    # sleep(0.5)  # 延迟，避免太快ip被封
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

def create_tcount():
    # 创建记录数据库表格操作次数的table
    global cursor
    cursor.execute("""create table if not exists t_count
                    (opera_count int)
                """)
    cursor.execute("select opera_count from t_count;")
    if cursor.fetchone() == None:
        cursor.execute("insert into t_count set opera_count={};".format(1))
    conn.commit()

def save_count():
    global cursor,conn
    cursor.execute("select opera_count from t_count;")
    operacount = cursor.fetchone()[0]
    cursor.execute("update t_count set opera_count=%d;" % (operacount+1))
    return operacount

def create_table_bili_video(TABLE_INDEX):
    # 创建数据库
    global cursor
    cursor.execute("""create table if not exists bili_video_{}
                    (v_aid int primary key,
                    v_title text,
                    v_biaoqian text,
                    v_author_mid int,
                    v_author_name text,
                    v_view1 int,v_view2 int,v_view3 int,v_view4 int,v_view5 int,v_view6 int,v_view7 int,v_view8 int,
                    v_danmu1 int,v_danmu2 int,v_danmu3 int,v_danmu4 int,v_danmu5 int,v_danmu6 int,v_danmu7 int,v_danmu8 int,
                    v_reply1 int,v_reply2 int,v_reply3 int,v_reply4 int,v_reply5 int,v_reply6 int,v_reply7 int,v_reply8 int,
                    v_favor1 int,v_favor2 int,v_favor3 int,v_favor4 int,v_favor5 int,v_favor6 int,v_favor7 int,v_favor8 int,
                    v_coin1 int,v_coin2 int,v_coin3 int,v_coin4 int,v_coin5 int,v_coin6 int,v_coin7 int,v_coin8 int,
                    v_share1 int,v_share2 int,v_share3 int,v_share4 int,v_share5 int,v_share6 int,v_share7 int,v_share8 int,
                    v_like1 int,v_like2 int,v_like3 int,v_like4 int,v_like5 int,v_like6 int,v_like7 int,v_like8 int,
                    v_dislike1 int,v_dislike2 int,v_dislike3 int,v_dislike4 int,v_dislike5 int,v_dislike6 int,v_dislike7 int,v_dislike8 int
                    )
                """.format(TABLE_INDEX))
                
def save_video_db(video_instance_per_page,table_index):
    # 将数据保存至服务器MySQL数据库中
    global cursor,conn
    for video_instance in video_instance_per_page:  # 对于每一个video对象，首先查询是否已经在第table_index存在，如果不存在--insert，如果存在--update
        for v_num in range(1,9):  # 查询第几行的v_view为空，将新值插入
            cursor.execute("select v_view{0} from bili_video_{1} where v_aid={2};".format(v_num,table_index,video_instance.aid))
            fet = cursor.fetchone()
            print("fet:%s"%fet)
            # print(video_instance.aid)
            if fet == None:  # 如果某个aid号还没有加到表中
                sql_ins = "insert into bili_video_%s set v_aid=%s,v_title=%s,\
                v_biaoqian=%s,v_author_mid=%s,v_author_name=%s,v_view%s=%s,\
                v_danmu%s=%s,v_reply%s=%s,v_favor%s=%s,v_coin%s=%s,v_share%s=%s,\
                v_like%s=%s,v_dislike%s=%s;"
                row1 = [table_index,
                        video_instance.aid,
                        video_instance.title,
                        video_instance.biaoqian,
                        video_instance.author_mid,
                        video_instance.author_name,
                        v_num,
                        video_instance.view,
                        v_num,
                        video_instance.danmu,
                        v_num,
                        video_instance.reply,
                        v_num,
                        video_instance.shoucang,
                        v_num,
                        video_instance.coin,
                        v_num,
                        video_instance.share,
                        v_num,
                        video_instance.like,
                        v_num,
                        video_instance.dislike
                        ]
                try:
                    # 执行sql_ins语句
                    cursor.execute(sql_ins,row1)
                except:
                    # 发生错误时回滚
                    conn.rollback()
                # 提交到数据库执行
                conn.commit()
                break
            else:
                data = fet[0]  # 返回某aid对应的view值，若为空，则跳出循环，并获得view序号
                if not data:
                    break
        print("#################")
        if v_num > 1 and data == None:  # 此aid已经有数据，更新值为NULL的值，即添加数据到view(num1)
            sql_update = "update bili_video_%s set v_view%s=%s,v_danmu%s=%s,\
            v_reply%s=%s,v_favor%s=%s,v_coin%s=%s,v_share%s=%s,v_like%s=%s,\
            v_dislike%s=%s where v_aid=%s;"
            row2 = [table_index,
                    v_num,
                    video_instance.view,
                    v_num,
                    video_instance.danmu,
                    v_num,
                    video_instance.reply,
                    v_num,
                    video_instance.shoucang,
                    v_num,
                    video_instance.coin,
                    v_num,
                    video_instance.share,
                    v_num,
                    video_instance.like,
                    v_num,
                    video_instance.dislike,
                    video_instance.aid
                    ]
            try:
                cursor.execute(sql_update,row2)
            except:
                conn.rollback()
            conn.commit()

def task_bili():
    global cursor,conn,video_info_all,URL_zone
    # 将数据存入数据库
    # 建立和数据库系统的连接
    conn = pymysql.connect(host=HOST_IP,port=3306,user=USER,password=USER_PASSWD,database='video_db',charset='utf8')
    # 创建游标
    cursor = conn.cursor()
    # 查表获取此次运行的序号（返回值为int，表示此次是第几次运行）
    create_tcount()
    call_count = save_count()
    if call_count%8 == 1:  # 满8次创建一个新表
        # 创建数据库中表格bili_video
        table_index = call_count//8 + 1
        create_table_bili_video(table_index)
    
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
    # 确定此次要在第几个表上进行存储操作
    if call_count%8 != 0:
        t_index = call_count//8 + 1
    else:
        t_index = call_count//8
    # 将所有视频信息传入数据库
    for video_per_page in video_info_all:
        save_video_db(video_per_page,t_index)
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

# 循环执行
def interval_trigger():
    global scheduler
    scheduler.add_job(func=task_bili, trigger='interval', seconds=30, id='interval_job1')  #返回一个apscheduler.job.Job的实例，可以用来改变或者移除job


if __name__ == "__main__":
    # interval_trigger()
    # try:
    #     scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     scheduler.shutdown() #关闭job
    # sleep(150)
    task_bili()
