from threading import Thread, current_thread

class TestThread:
    def method_inside_class(self, n):
        print('8888888', current_thread())
        for i in range(n):
            print('pppppppppp')

    @classmethod
    def method_inside_class_class_method(self, n):
        print('777777', current_thread())
        for i in range(n):
            print('cccccccc')

    @staticmethod
    def method_inside_class_class_static(n):
        print('k', current_thread())
        for i in range(n):
            print('aaaaaaaa')

cls1 = TestThread()
obj1 = Thread(target=cls1.method_inside_class, args=(5,))
obj2 = Thread(target=TestThread.method_inside_class_class_method, args=(5,))
obj3 = Thread(target=TestThread.method_inside_class_class_static, args=(5,))
obj1.start()
obj2.start()
obj3.start()
for i in range(5):
    print('------------tttt------------')

# def fun(n, m, j):
    
