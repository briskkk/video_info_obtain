# -*- coding:UTF-8 -*-
 
# 获取单机游戏模块按投稿时间排序中，连续10000个视频的播放量等信息
# 单机游戏模块tid=17，按热度排序为newlist，脚本为周期性运行脚本，会将每次运行收集到的数据存入数据库
# 脚本第一次运行：
# 先访问page=1的信息，获取到当前视频总数N，按每页50个获取前200页的视频信息（共200×5=10000个）
# 同时要保存第1个视频处于列表的倒数第几个视频（M=N），存到数据库中，用于之后脚本寻找该视频，因为newlist视频倒着数的排名不会变
# 对每个视频建立video对象用来存视频播放量等数据，然后将全部信息获取到列表video_info_all中，再写入数据库
# 脚本第n次运行：
# 先访问page=1的信息，获取到当前视频总数Nn，按每页50个计算总页数，然后第一次获取的10000个视频中，第一个视频的位置为（Mn=Nn-M+1）
# 计算出第一个视频在第几页（Mn//50 if Mn%50!=0 then +1），第10000个视频往后推200页（共收集201页数据）
# 然后根据视频的aid号和数据库中已经存入的aid号做对比，如果相同将新的数据填入表中
# 数据库中共有两类表格：
# 1.t_count -- 1个，用于存脚本运行的次数以及每次运行的时间戳，同时存第一次视频总数N
# 2.bili_video_n -- n个，用于存视频信息，每行代表一个视频，一个表可以存放8次视频播放信息，每过8次新建一个表格继续存数据
# 每隔3h运行一次，每分钟发起近200次http请求，待优化
# 相比v1增加了时间戳，并且信息部分增加了上传时间和视频时长信息

import os
import json
import requests
import platform
from time import *
from multiprocessing.dummy import Pool as ThreadPool
from apscheduler.schedulers.background import BackgroundScheduler
from video_classes import *
import pymysql
from secret import *
from datetime import datetime

video_info_all = []
scheduler = BackgroundScheduler()
URL_zone = "https://api.bilibili.com/x/web-interface/newlist?rid=17&type=0"

# 先访问page1，然后可以获取到一共有多少个视频，并写入数据库。
def get_video_num(URL_zone,page=1):
    URL_PAGE1 = URL_zone + "&pn={0}&ps=20".format(page)
    response1 = requests.get(url = URL_PAGE1,headers=headers())
    if response1.status_code !=200:
        sleep(30)  # 如果返回值不是200则等待30s再次访问
        response1 = requests.get(url = URL_PAGE1,headers=headers())
    response1.raise_for_status()  # 如果两次访问后返回值不是200则抛出异常终止程序
    VID_INFO1 = response1.json()  # 从第一页获取到的前二十个视频列表
    VIDEO_NUM = VID_INFO1['data']['page']['count']  # 视频总数
    cursor.execute("select count(opera_count) from t_count;")
    operacount = cursor.fetchone()[0]
    cursor.execute("update t_count set video_num=%d where opera_count=%d" % (VIDEO_NUM,operacount))
    return VIDEO_NUM
    
# get_video_info -- 获取某个页码上的视频对象信息，返回每页的视频对象列表（每页50个）
def get_video_info(page_num):
    global URL_zone,cursor,conn
    video_info_list = []
    URL_PAGE = URL_zone + "&pn={0}&ps=50".format(page_num)
    response = requests.get(url = URL_PAGE,headers=headers())
    if response.status_code !=200:
        sleep(30)  # 如果返回值不是200则等待30s再次访问
        response = requests.get(url = URL_PAGE,headers=headers())
    response.raise_for_status()  # 如果两次访问后返回值不是200则抛出异常终止程序
    VID_INFO = response.json()  # 从第page_count页获取视频列表
    sleep(1.5)  # 延迟，避免太快ip被封
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
        video.pubtime = video_info['pubdate']  # 发布时间
        video.duration = video_info['duration']  # 视频时长
        video_info_list.append(video)
    return video_info_list  # 返回视频对象列表
    
# 已完成视频信息的读取以及多线程的实现，接下来需要把信息存到数据库，周期运行代码功能。

def create_tcount():
    # 创建记录数据库表格操作次数的table
    global cursor
    cursor.execute("""create table if not exists t_count
                    (opera_count int primary key,
                    opera_time text,
                    video_num int)
                """)
    cursor.execute("select count(opera_count) from t_count;")
    if cursor.fetchone()[0] == 0:
        cursor.execute("insert into t_count set opera_count={};".format(1))
    conn.commit()

def save_count():
    global cursor,conn
    cursor.execute("select count(opera_count) from t_count;")
    operacount = cursor.fetchone()[0]
    dt=datetime.now()
    cursor.execute("update t_count set opera_time=%s where opera_count=%d;" % (dt.timestamp(),operacount))
    cursor.execute("insert into t_count set opera_count={};".format(operacount+1))
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
                    v_pubdate int,
                    v_duration int,
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
                
def save_video_db(video_instance_per_page,table_index,script_call_count):
    # 将数据保存至服务器MySQL数据库中
    global cursor,conn
    if script_call_count == 1:  # 如果是第一次运行，则insert
        for video_instance in video_instance_per_page:  # 对于每一个video对象，首先查询是否已经在第table_index存在，如果不存在--insert
            cursor.execute("select * from bili_video_1 where v_aid={1};".format(video_instance.aid))
            fet = cursor.fetchone()
            if fet == None:  # 如果某个aid号还没有加到表中，为了避免两页有同样的视频号重复填入，保持第一次数据
                sql_ins = "insert into bili_video_1 set v_aid=%s,v_title=%s,\
                v_biaoqian=%s,v_author_mid=%s,v_author_name=%s,v_pubdate=%s,\
                v_duration=%sv_view1=%s,v_danmu1=%s,v_reply1=%s,v_favor1=%s,\
                v_coin1=%s,v_share1=%s,v_like1=%s,v_dislike1=%s;"
                row1 = [video_instance.aid,
                        video_instance.title,
                        video_instance.biaoqian,
                        video_instance.author_mid,
                        video_instance.author_name,
                        video_instance.pubtime,
                        video_instance.duration,
                        video_instance.view,
                        video_instance.danmu,
                        video_instance.reply,
                        video_instance.shoucang,
                        video_instance.coin,
                        video_instance.share,
                        video_instance.like,
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
    elif script_call_count != 1 and script_call_count%8 == 1:  # 第n次运行，如果是新表（script_call_count%8 == 1） -- insert，否则 -- update
        # 首先把第一张表的v_aid等不会变动的信息拷贝到这张表上
        cursor.execute("insert into video_db.bili_video_{0}(v_aid) select v_aid from video_db.bili_video_1;".format(table_index))
        cursor.execute("insert into video_db.bili_video_{0}(v_title) select v_title from video_db.bili_video_1;".format(table_index))
        cursor.execute("insert into video_db.bili_video_{0}(v_biaoqian) select v_biaoqian from video_db.bili_video_1;".format(table_index))
        cursor.execute("insert into video_db.bili_video_{0}(v_author_mid) select v_author_mid from video_db.bili_video_1;".format(table_index))
        cursor.execute("insert into video_db.bili_video_{0}(v_author_name) select v_author_name from video_db.bili_video_1;".format(table_index))
        cursor.execute("insert into video_db.bili_video_{0}(v_pubdate) select v_pubdate from video_db.bili_video_1;".format(table_index))
        cursor.execute("insert into video_db.bili_video_{0}(v_duration) select v_duration from video_db.bili_video_1;".format(table_index))
        for video_instance in video_instance_per_page:  # 对于每一个video对象 -- update
            cursor.execute("select * from bili_video_{0} where v_aid={1};".format(table_index,video_instance.aid))
            fet = cursor.fetchone()
            if fet != None:  # 如果某个aid号不在表中，则丢弃；若在表中，则update
                sql_update1 = "update bili_video_%s set v_view1=%s,v_danmu1=%s,\
                v_reply1=%s,v_favor1=%s,v_coin1=%s,v_share1=%s,v_like1=%s,\
                v_dislike1=%s where v_aid=%s;"
                row2 = [table_index,
                        video_instance.view,
                        video_instance.danmu,
                        video_instance.reply,
                        video_instance.shoucang,
                        video_instance.coin,
                        video_instance.share,
                        video_instance.like,
                        video_instance.dislike,
                        video_instance.aid
                        ]
                try:
                    cursor.execute(sql_update1,row2)
                except:
                    conn.rollback()
                conn.commit()
    else:  # update
        for video_instance in video_instance_per_page:  # 对于每一个video对象，首先查询是否已经在第table_index存在，如果不存在--insert，如果存在--update
            cursor.execute("select * from bili_video_{0} where v_aid={1};".format(table_index,video_instance.aid))
            fet = cursor.fetchone()
            if script_call_count%8  == 0:  # 判断这是第几个表的第几次操作，要填到对应v_view的第几列
                v_colomn = 8
            else:
                v_colomn = script_call_count%8
            if fet != None:  # 如果某个aid号在表中，update
                sql_update2 = "update bili_video_%s set v_view%s=%s,v_danmu%s=%s,\
                v_reply%s=%s,v_favor%s=%s,v_coin%s=%s,v_share%s=%s,v_like%s=%s,\
                v_dislike%s=%s where v_aid=%s;"
                row2 = [table_index,
                        v_colomn,
                        video_instance.view,
                        v_colomn,
                        video_instance.danmu,
                        v_colomn,
                        video_instance.reply,
                        v_colomn,
                        video_instance.shoucang,
                        v_colomn,
                        video_instance.coin,
                        v_colomn,
                        video_instance.share,
                        v_colomn,
                        video_instance.like,
                        v_colomn,
                        video_instance.dislike,
                        video_instance.aid
                        ]
                try:
                    cursor.execute(sql_update2,row2)
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
    
    video_num_n = get_video_num(URL_zone)  # 获取本次操作视频排行总视频数
    print("#######################")
    starttime = time()
    # pagelist = range(1,num_of_pages+1)
    if call_count == 1:
        pagelist = range(1,201)
    else:
        cursor.execute("select video_num from t_count where operacount=1;")
        video_num_1 = cursor.fetchone()[0]
        order_n = (int(video_num_n)-int(video_num_1)+1)
        if order_n%50 == 0:
            start_n = order_n//50
        else:
            start_n = order_n//50 + 1
        pagelist = range(start_n,start_n+202)
    pool = ThreadPool(4)
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
        save_video_db(video_per_page,t_index,call_count)
    endtime = time()
    print('Use time is ',(endtime-starttime))
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
    scheduler.add_job(func=task_bili, trigger='interval', minutes=30, id='interval_job1')  #返回一个apscheduler.job.Job的实例，可以用来改变或者移除job


if __name__ == "__main__":
    task_bili()
    # interval_trigger()
    # try:
    #     scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     scheduler.shutdown() #关闭job
    # sleep(36000)

