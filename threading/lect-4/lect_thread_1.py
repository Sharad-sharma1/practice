from threading import Thread, current_thread


def name_print(n: int, namee: dict):
    print('-0-0-0-', current_thread())
    print('-0-0-0-', current_thread().ident)
    print('-0-0-0-', current_thread().name)
    for i in range(n):
        print('------', namee)

# print(name_print({'n':6, 'namee':'hjdhdhd'}))
obj = Thread(target=name_print, kwargs={'n':3, 'namee':"shshshshs"})
obj.start()

name_print(3, 'sharad')