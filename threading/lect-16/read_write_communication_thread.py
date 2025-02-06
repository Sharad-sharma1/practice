import threading 
e = threading.Event()
import time
def read_file():
    for i in range(1,21):
        time.sleep(1)
        print(f'read line {i}')
        if i%5==0:
            e.set()


def write_file():
    for i in range(1,21):
        e.wait()
        time.sleep(0.5)
        print(f'write line {i}')
        if i%5==0:
            e.clear()

t1 = threading.Thread(target=read_file, name="thread 1")
t2 = threading.Thread(target=write_file, name="thread 2")
t1.start()
t2.start()