from threading import Thread

def thread_func1():
    for i in range(4):
        print('---------thread_func1----------', i)

def thread_func2():
    for i in range(4):
        print('---------thread_func2----------', i)

t1 = Thread(target=thread_func1)
t2 = Thread(target=thread_func2)
t1.start()
t2.start()
print("--name t1---", t1.name)
print("--name t2---", t2.name)
# ------------------- we can also use some functions to perform basic operations-----------
# Way:- 1
t1.name = "Sharad Thread1"
t2.name = "Sharad Thread2"
print("--name t1 after name change---", t1.name)
print("--name t2 after name change---", t2.getName())
# Way:- 2
t1.setName = "Sharad setname Thread1"
t2.setName = "Sharad setname Thread2"
print("--name t1 setname name change---", t1.name)
print("--name t2 setname name change---", t2.getName())

import os
# --------- we can fetch id of each thread -----------
print("-- ID t1 ---", t1.ident)
print("-- ID t2 ---", t2.native_id)
print("-- ID t2 ---", os.getpid())
# ----------- above both are same but both are different identifier and pid is also different --------------------
