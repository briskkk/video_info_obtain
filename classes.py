# -*- coding:UTF-8 -*-
class Video():
    def __init__(self,video_aid=None,video_title=None):
        if video_aid:
            self.aid = video_aid
        if video_title:
            if isinstance(video_title,unicode):
                video_title = video_title.encode('utf8')
            self.title = video_title
    aid = None
    title = None
    view = None
    shoucang = None
    danmu = None
    date = None
    cover = None
    reply = None
    description = None
    share = None
    like = None
    dislike = None
    tag = None
    author_mid = None
    author_name = None
    page = None
    credit = None
    coin = None
    spid = None
    cid = None
    Iscopy = None
    subtitle = None
    duration = None
    episode = None
    arcurl = None#网页地址
    tid = None
    typename = None