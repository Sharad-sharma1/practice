import threading
e = threading.Event()
import time

def ok():
    print("Green signal")
    # print("tell people to start crossing")
    e.set()
    time.sleep(3)
    print("light is red")
    e.clear()
    time.sleep(3)

    print("Green signal")
    # print("tell people to start crossing")
    e.set()
    time.sleep(3)
    print("light is red")
    e.clear()
    time.sleep(3)


def ok2():
    e.wait()
    while e.is_set():
        print("---------++++++++++++++| you can can crossing the road |++++++++++++++--------------")
        time.sleep(1)
        e.wait()

t1 = threading.Thread(target=ok)
t2 = threading.Thread(target=ok2)
t1.start()
t2.start()