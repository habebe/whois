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
            if line.find("=") == -1:
                if line.find("%") == -1:
                    data = line.split(";")
                    L = ""
                    lcounter = 0
                    for i in data:
                        L += i.strip()
                        lcounter += 1
                        if lcounter < len(data):
                            L += ";"
                            pass
                        pass
                    print >>fout,L
                    pass
                pass
            pass
        line = fin.readline()
        pass
    pass

read(sys.argv[1],int(sys.argv[2]))
