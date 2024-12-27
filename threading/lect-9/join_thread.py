from threading import Thread
import time

def vid_up():
    print('-------vid up staerted---')
    time.sleep(3)
    print('----vid com--------')

def send_noti():
    for i in range(6):
        print('--------noitifcation sent---------')

# t1 = Thread(target=vid_up)
vid_up()
t2 = Thread(target=send_noti)
# t1.start()
# t1.join() # this will let complete the execution of vid_up then t2 will start with thread 2 and main thread
t2.start()
for i in range(10):
    print('------=========-----------==========')