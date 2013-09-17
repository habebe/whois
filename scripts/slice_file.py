import os
import sys


def read(fileName,limit):
    fin = file(fileName,"r")
    fout = file("{0}.{1}".format(fileName,limit),"w")
    line = fin.readline()
    done = False
    counter = 0
    while not done:
        counter += 1
        done = (len(line) == 0) or (counter > limit)
        if not done:
            print >> fout,line,
        line = fin.readline()
        pass
    pass

read(sys.argv[1],int(sys.argv[2]))
