from threading import Thread, current_thread, Lock
mylock = Lock()
def test():
    for i in range(5):
        print("welcomee")
    mylock.acquire()
    print("done")
    import time
    time.sleep(3)
    mylock.release()

t1 = Thread(target=test)
t2 = Thread(target=test)
t1.start()
t2.start()

