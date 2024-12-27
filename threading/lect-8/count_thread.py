from threading import Thread, current_thread, active_count, enumerate

def ok():
    for i in range(6):
        print('------------ok=----------', i)

for i in range(6):
    print('--------out-------------', i)

new = Thread(target=ok)
print('beffffff', new.is_alive())
new.start()
print('0000000000', current_thread())
print('0000000000', enumerate())
print('0000000000', active_count())
print('afffffffff', new.is_alive())

