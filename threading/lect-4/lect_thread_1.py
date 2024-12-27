from threading import Thread, current_thread


def name_print(n: int, namee: dict, func_name = 'mainnn'):
    print(f'-0-0-{func_name}-0-', current_thread())
    print(f'-0-0-{func_name}-0-', current_thread().ident)
    print(f'-0-0-{func_name}-0-', current_thread().name)
    for i in range(n):
        print('------', namee)

# print(name_print({'n':6, 'namee':'hjdhdhd'}))
obj = Thread(target=name_print, kwargs={'n':6, 'namee':"shshshshs"})
obj.start()

name_print(6, 'sharad', 'sharadddddd')