import os
import time
import sys

if __name__ == '__main__':
    a = sys.argv[1]
    time.sleep(20)
    f = open(a, 'wt')
    f.write("1" + '\n')
    f.write("2" + '\n')
    f.close()