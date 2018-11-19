from datetime import datetime
import time
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler(daemon=False)

def alarm(type):
    print('[%s Alarm] This alarm was scheduled at %s.' % (type, datetime.now().strftime('%Y-%m-%d %H-%M-%S')))

# 循环执行
def interval_trigger():
    global scheduler
    scheduler.add_job(func=alarm, args=['interval'], trigger='interval', seconds=2, id='interval_job1')  #返回一个apscheduler.job.Job的实例，可以用来改变或者移除job

if __name__ == "__main__":
    #scheduler.add_job(timedTask,'interval',hours=3)
    interval_trigger()
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown() #关闭job