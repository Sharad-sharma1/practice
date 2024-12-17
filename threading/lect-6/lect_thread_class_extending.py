from threading import Thread
import time
video_list = ['beatboxing', 'caisthenics', 'gym', 'coding']
class ExtendThreadClass(Thread):
    def __init__(self, child_var):
        Thread.__init__(self)
        self.child = child_var
        # self.total = 0
    def run(self):
        a,b = 10, 20
        if self.child:
            print('-=-=-=-=-=-=', self.child)
        for i in video_list:
            print('---**** video started uploading ***----', i)
            time.sleep(2)
            print('---**** video uploaded ***----', i)
        self.total = a+b
obj1 = ExtendThreadClass(child_var=False)
obj1.start()
# time.sleep(9)
for i in range(len(video_list)):
    time.sleep(2)
    print('-----copyright checking-----', i)
print('---------totallllllllllll--------------------', obj1.total)
