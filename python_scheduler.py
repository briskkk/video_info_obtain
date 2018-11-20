from datetime import datetime
import time
from apscheduler.schedulers.background import BackgroundScheduler
import pymysql
from secret import *

scheduler = BackgroundScheduler()
def create_video_db(TABLE_INDEX):
    # 创建数据库表格test
    global cursor
    print(TABLE_INDEX)
    cursor.execute("""create table if not exists test_{0}
                    (aid int primary key,
                    view1 int,view2 int,view3 int,view4 int
                    )
                """.format(TABLE_INDEX))

def create_tcount():
    # 创建记录数据库表格操作次数的table
    global cursor
    cursor.execute("""create table if not exists t_count
                    (opera_count int)
                """)
    cursor.execute("select opera_count from t_count")
    if cursor.fetchone() == None:
        cursor.execute("insert into t_count set opera_count={}".format(1))
    conn.commit()

def save_count():
    global cursor,conn
    cursor.execute("select opera_count from t_count")
    operacount = cursor.fetchone()[0]
    cursor.execute("update t_count set opera_count=%d" % (operacount+1))
    return operacount

def save_video_db(TABLE_INDEX):
    # 将数据保存至远端数据库表格test中
    global cursor,conn
    data = []
    aid = 111
    for num1 in range(1,5):
        cursor.execute("select view{0} from test_{1} where aid={2}".format(num1,TABLE_INDEX,aid))
        fet = cursor.fetchone()
        if fet == None:  # 如果某个aid号还没有加到表中
            print("######")
            sql = "insert into test_{0} set aid=%s, view{1}=%s;".format(TABLE_INDEX,1)
            row = [aid,num1]
            try:
                # 执行sql语句
                cursor.execute(sql,row)
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
    print("data:{}".format(data))
    print("view{}".format(num1))

    if num1 > 1 and data == None:  # 此aid已经有数据，更新值为NULL的值，即添加数据到view(num1)
        sql = "update test_{0} set view{1}=%s".format(TABLE_INDEX,num1)
        row = [num1,]
        try:
            cursor.execute(sql,row)
        except:
            conn.rollback()
        conn.commit()
    # elif num1 > 1 and data != None:  # 此表格预留的空位已经填满，需要新建一个表格继续填写
    #     TABLE_INDEX = TABLE_INDEX + 1
    #     create_video_db(TABLE_INDEX)
    #     sql = "insert into test_{0} set aid=%s, view{1}=%s;".format(TABLE_INDEX,num1)
    #     row = [111,1]
    #     try:
    #         # 执行sql语句
    #         cursor.execute(sql,row)
    #     except:
    #         # 发生错误时回滚
    #         conn.rollback()
    #     # 提交到数据库执行
    #     conn.commit()

def main2():
    global cursor,conn
    # 将数据存入数据库
    # 建立和数据库系统的连接
    conn = pymysql.connect(host=HOST_IP,port=3306,user=USER,password=USER_PASSWD,database='video_db',charset='utf8')
    # 创建游标
    cursor = conn.cursor()
    create_tcount()
    call_count = save_count()
    if call_count%4 == 1:  # 满4次创建一个新表
        # 创建数据库中表格bili_video
        table_index = call_count//4 + 1
        create_video_db(table_index)
    
    # 将所有视频信息传入数据库
    if call_count%4 != 0:
        save_video_db(call_count//4 + 1)
    else:
        save_video_db(call_count//4)
    # 关闭游标
    cursor.close()
    # 关闭连接
    conn.close()

# 循环执行
def interval_trigger():
    global scheduler
    scheduler.add_job(func=main2, trigger='interval', seconds=5, id='interval_job1')  #返回一个apscheduler.job.Job的实例，可以用来改变或者移除job

        
if __name__ == "__main__":
    #scheduler.add_job(timedTask,'interval',hours=3)
    interval_trigger()
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown() #关闭job
    time.sleep(60)