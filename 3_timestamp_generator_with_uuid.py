from random import randint
import datetime
import time
import uuid

file_idx = 1

try:
    while True: 
        with open("event-time-data/%u.csv" % file_idx, "w") as f:
            for i in range(10):
                rec = f"{uuid.uuid1()}\t{datetime.datetime.now()}\t{randint(0,100)}\n"
                print(rec)
                f.write(rec)

        file_idx += 1
        time.sleep(3)
            
except KeyboardInterrupt:
    pass

