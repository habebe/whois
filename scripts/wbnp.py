import os
import sys
import getopt
import shutil
import os
import csv
from urlparse import urlparse

def write_data(outputFile,domain,volume,ip):
    print >> outputFile,"B;{0};{1};{2}".format(domain,volume,ip)
    pass

def get_volume(row):
    try:
        volume     = float(row[2])
        rate       = float(row[3])
        volume     = volume/rate
        return volume
    except:
        return None

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
        S = h.split('\t')
        counter = 0
        for i in S:
            print counter,i
            counter += 1
            pass
        pass
    reader = csv.reader(inputFile,delimiter='\t')
    outputFile = file(outputFileName,"w")
    done = False
    counter = 0
    limit = 100
    while not done:
        try:
            for row in reader:
                domain     = row[1]
                volume     = get_volume(row)
                ip         = row[16]
                counter += 1
                url = urlparse(domain)
                domain = url.netloc
                index = domain.rfind(':')
                if index > 0:
                    domain = domain[:index]
                    pass
                count = domain.count(".")
                while count >= 2:
                    index = domain.find(".")
                    domain = domain[index+1:]
                    count = domain.count(".")
                    pass                
                domain = domain.strip()
                if len(domain) and (ip) and len(ip) and (volume != None):
                    write_data(outputFile,domain,volume,ip)
                    pass
                else:
                    pass

                if (counter % 100000) == 0:
                    print counter
                    pass
                pass
            done = True
            pass
        except:
            print "ERROR",sys.exc_info()
            pass
        pass
    outputFile.close()
    inputFile.close()
    return True


main(sys.argv[1],sys.argv[2],int(sys.argv[3]))
