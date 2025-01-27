from threading import Semaphore, BoundedSemaphore, Thread, current_thread
import time
s_obj = Semaphore(3) # if we dont pass any number init, it will process one at a time. max 3 it takes
# also note that if we do 2 acquire and 2 realse no issues, but if the release and acquire are inconsistent
# it will cause problem because reason semaphore, keeps a counter and max at 3
# so acquire keep 3-1 = 2 and release again adds in the same counter 2+1 = 3
# so if release and acquire are in consistent it will result failure

bs_obj = BoundedSemaphore()
# eveything is same but it resolves the counter, release and aquire problem

t_obj = Thread

def main():
    s_obj.acquire()
    s_obj.acquire()
    s_obj.acquire()
    print('started', current_thread().name)
    time.sleep(2)
    print('workinded', current_thread().name)
    s_obj.release()

t1 = t_obj(target=main, name="thread 1")
t2 = t_obj(target=main, name="thread 2")
t3 = t_obj(target=main, name="thread 3")
t4 = t_obj(target=main, name="thread 4")
t5 = t_obj(target=main, name="thread 5")
t1.start()
t2.start()
t3.start()
t4.start()
t5.start()