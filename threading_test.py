# -*- coding:UTF-8 -*-
 
# 这个程序用多线程来获取b站视频中播放量过1k的av号(从1-1000遍历aid号)
# 最后写入txt文件中：hot_video.txt
# 例如：
# {"code":0,"message":"0","ttl":1,
# "data":{"aid":1,"view":"--","danmaku":10000,"reply":10000,"favorite":10000,"coin":1000,"share":10000,
# "now_rank":0,"his_rank":173,"like":10000,"dislike":1,"no_reprint":0,"copyright":2}}

import os
import json
import requests
import platform
from time import *
from multiprocessing import *
import threading


class MyThread(threading.Thread):

    def __init__(self,func,args=()):
        super(MyThread,self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None

def main1():
    S_AID = 1 # 起始aid号
    E_AID = 1000 # 结束aid号
    
    print("#######################")
    starttime = time()
    aidlist = range(S_AID,E_AID+1)
    results = []
    results2 = []
    # lock = threading.Lock()
    for t in aidlist:
        task = MyThread(require_video,args=(t,))
        results.append(task)
        task.start()
    for m in results:
        m.join()
        results2.append(m.get_result())
        print(results2)
    parse_video(results2)
    endtime = time()
    print('All subprocesses done.')
    print('Use time is ',(endtime-starttime))
    # global timer
    # timer = threading.Timer(30, main1)
    # timer.start()

    

def require_video(video_id):
    URL_VIDinfo = "http://api.bilibili.com/archive_stat/stat?aid="   #b站视频信息api
    PARAMS = {"aid":video_id }
    VID_info = requests.get(url = URL_VIDinfo,params = PARAMS).json()
    if(VID_info["message"] == "0"):
        hot_video = VID_info["data"]
        favorite = hot_video["favorite"]
        danmaku = hot_video["danmaku"]
        coin = hot_video["coin"]
        view = hot_video["view"]
        share = hot_video["share"]
        reply = hot_video["reply"]
        like = hot_video["like"]
        dislike = hot_video["dislike"]
        
        if view != "--":
            # print([video_id,favorite,danmaku,coin,view,share,reply,like,dislike])
            return [video_id,favorite,danmaku,coin,view,share,reply,like,dislike]
        else:
            return [video_id,-1]
    else:
        return [video_id,-1]
        
def parse_video(video_messages_list):
    num_of_hot_video = 0
    number = 1
    f = open("hot_video.txt" , "w")
    f.write("***********************************************\n")
    # for video_id in range(6,100):
        # video = require_video(video_id)
        # print(video)
    for video in video_messages_list:
        if(video[1] == -1):
            # print("第" + str(number) + "个视频不存在")
            number += 1
        elif(int(video[4]) >= 100000):
            num_of_hot_video += 1
            f.write("av号：" + str(video[0]) + "播放量：" + str(video[4]) + "\n")
            f.write("收藏：" + str(video[1]) + "\n")
            f.write("弹幕数：" + str(video[2]) + "\n")
            f.write("硬币数：" + str(video[3]) + "\n")
            f.write("分享数：" + str(video[5]) + "\n")
            f.write("评论数：" + str(video[6]) + "\n")
            f.write("顶：" + str(video[7]) + "\n")
            f.write("踩：" + str(video[8]) + "\n\n")
            print("第" + str(number) + "个视频的播放量为：" + str(video[4]))
            number += 1
        else:
            number += 1
    f.write("************************************************\n")
    f.close()
    print("av号从1开始的视频中播放数超过1k的视频有" + str(num_of_hot_video) + "个")


if __name__ == "__main__":
    main1()
