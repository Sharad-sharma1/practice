from threading import Thread, Lock, RLock
l = Lock()
lr = RLock()
def f1():
    l.acquire()
    print("sharad f1")
    l.release()

def f2():
    l.acquire()
    print("sharad f2")
    l.release()

def main():
    # l.acquire() here if i dont know the fucntion is having lock then i try to use lock again
    # then it will stuck the terminal that's why we will use rlock
    lr.acquire()
    f1()
    f2()
    print("sharad main")
    lr.release()
    # l.release() here it wont block the terminal instead it will throw an error

main()

t1 = Thread(target=main)
t2 = Thread(target=main)
t1.start()
t2.start()