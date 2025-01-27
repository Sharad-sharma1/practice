import threading
e = threading.Event()
def ok():
    import time
    print("game start")
    time.sleep(3)
    print("yooooo thread 2, please kill the fucn")
    e.set()

def ok2():
    e.wait()
    if e.is_set():
        print("-----------killing the process")

t1 = threading.Thread(target=ok)
t2 = threading.Thread(target=ok2)
t1.start()
t2.start()