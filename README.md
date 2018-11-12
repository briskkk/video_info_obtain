# learngit

**分别使用多线程和多进程，通过api获取视频播放量等信息，最后将获取到的信息筛选并输出到txt文本中。**
----
### 信息格式：
```
{"code":0,"message":"0","ttl":1,
"data":{"aid":2,"view":"--","danmaku":24072,"reply":26725,"favorite":11729,"coin":4627,"share":2860,
"now_rank":0,"his_rank":173,"like":7885,"dislike":102,"no_reprint":0,"copyright":2}}
```
### 信息输出到文本中的格式：
```
av号：2播放量：446105
收藏：11729
弹幕数：24072
硬币数：4627
分享数：2860
评论数：26725
顶：7885
踩：102
```	
**使用到的python库：multiprocessing 和 threading**  
&ensp;&ensp;multiprocessing为多进程库，可创建进程池pool。  
&ensp;&ensp;multiprocessing_map使用了pool中的map函数；  
&ensp;&ensp;multiprocessing_async使用了pool中的apply_async函数。  
&ensp;&ensp;threading为多线程库，  
&ensp;&ensp;threading_test创建多线程完成任务，但没有引入线程池概念。  

