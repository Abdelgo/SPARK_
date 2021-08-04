from random import randint
import datetime
import time

file_idx = 1

try:
    while True: 
        with open("event-time-data/%u.csv" % file_idx, "w") as f:
            for i in range(10):
                rec = "%s\t%u\n" % (datetime.datetime.now(),randint(0,100))
                print(rec)
                f.write(rec)

        file_idx += 1
        time.sleep(3)
            
except KeyboardInterrupt:
    pass
