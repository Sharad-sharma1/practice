from threading import Thread
import time
def squ_cub(num):
    print('-----------squ_cub-----------------')
    # time.sleep(1)
    ok = 80
    for i in range(5*num):
        ok += i
    
    print(f'The squre/cube of {ok} :-')
def squ_cub2(num):
    print('-----------squ_cub2222-----------------')
    # time.sleep(1)
    ok2 = 80
    for i in range(5*num):
        ok2 += i
    
    print(f'The squre/cube of {ok2} :-')

begin = time.time()
t1 = Thread(target=squ_cub, args=(100000000,))
t2 = Thread(target=squ_cub2, args=(130000000,))
t1.start()
t2.start()
# t1.join()
# t2.join()
print('----------------total-----time:------------', time.time()-begin)

# # -----------------------------without thread ------------------------------------#

print('# -----------------------------without thread ------------------------------------#')
begin2 = time.time()
squ_cub(100000000)
squ_cub2(130000000)
print('----------------total-----time:------------', time.time()-begin2)