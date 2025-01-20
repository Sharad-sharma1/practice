from threading import Thread, current_thread

class RaceCondition:
    def __init__(self, av_seats, name):
        self.av_seats = av_seats
    
    def res_seat(self, need_seats):
        print(f"----av seats-----------", self.av_seats)
        if self.av_seats < need_seats:
            print(f'{current_thread().name}-------seat is not available-------{self.av_seats}--')
        else:
            self.av_seats -= need_seats
            print(f'-----{self.av_seats}--seat is available--------')

t1_obj = RaceCondition(2,"cityflow bus")
t1 = Thread(target=t1_obj.res_seat, args=(1,), name="sharad")
t2 = Thread(target=t1_obj.res_seat, args=(2,), name="rahul")
t1.start()
t2.start()