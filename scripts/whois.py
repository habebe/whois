import os
import sys
import getopt
import shutil
import os
import csv

def write_data(outputFile,domain,registrar,email,nameServer,country):
    print >> outputFile,"A;{0};{1};{2};{3};{4}".format(domain,registrar,email,nameServer,country)
    pass


def main(inputFileName,outputFileName,header):
    if inputFileName == None:
        print "Input file name is not given."
        return False
    if outputFileName == None:
        print "Output file name is not given."
        return False
    if not os.path.exists(inputFileName):
        print "Input file name {0} does not exist.".format(inputFileName)
        return False

    inputFile = file(inputFileName,"rb")
    if header:
        h = inputFile.readline()
        S = h.split(",")
        counter = 0
        for i in S:
            print counter,i
            counter += 1
            pass
        pass
    reader = csv.reader(inputFile)
    outputFile = file(outputFileName,"w")
    done = False
    counter = 0
    while not done:
        try:
            for row in reader:
                domain     = row[0]
                registrar  = row[1]
                email      = row[2]
                nameServer = row[4]
                country    = "{0}.{1}".format(row[23],row[22])
                counter += 1
                write_data(outputFile,domain,registrar,email,nameServer,country)
                if (counter % 100000) == 0:
                    print counter
                    pass
                pass
            done = True
            pass
        except:
            print "ERROR",row
            pass
        pass
    outputFile.close()
    inputFile.close()
    return True


main(sys.argv[1],sys.argv[2],int(sys.argv[3]))
