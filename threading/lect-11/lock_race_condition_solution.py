from threading import Thread, current_thread, Lock

lock = Lock()

class bus:
    def __init__(self, name, seat_av, loc):
        self.name = name
        self.seat_av = seat_av
        self.loc = loc

    def reserve_seat(self, book_seats):
        self.loc.acquire()
        # self.loc.acquire() if try by mistakely to aquire again then this will cause problem in lock and this can
        # occur when we are using any other function and we dont know if that fucntion has lock or not
        print("seats left", self.seat_av)
        if self.seat_av<book_seats:
            print('no seats av', current_thread().name)
        else:
            self.seat_av -= book_seats
            print('seats av --- booked', current_thread().name)
        self.loc.release()
ibj_bus = bus("sharad bu service", 2, lock)
t1 = Thread(target=ibj_bus.reserve_seat, args=(1,), name="sharad")
t2 = Thread(target=ibj_bus.reserve_seat, args=(2,), name="rahul")
t1.start()
t2.start()
