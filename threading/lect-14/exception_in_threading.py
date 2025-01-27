import threading

def exceptt(args):
    print('---args==0-', args[0])    
    print('---args==1-', args[1])
    print('---args==2-', args[2])
    print('---args==3-', args[3])

def ok():
    print('8888888'+88)

threading.excepthook=exceptt
chil = threading.Thread(target=ok)
chil.start()
chil.join()
for i in [1,2,3,4]:
    print(i)