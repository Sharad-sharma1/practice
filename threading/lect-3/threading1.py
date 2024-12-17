import threading

print(threading.current_thread())
print(threading.current_thread().getName)
print(threading.current_thread().ident)