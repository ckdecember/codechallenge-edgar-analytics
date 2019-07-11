import argparse
import logging
import unittest

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch.setFormatter(formatter)
logger.addHandler(ch)

#python ./src/sessionization.py ./input/log.csv ./input/inactivity_period.txt ./output/sessionization.txt
# yikes, hardcoded spots.  ah wells.

class Sessionization:
    def __init__(self, logfile, inactivityfile, sessionizationfile):
        self.logfile = logfile
        self.inactivityfile = inactivityfile
        self.sessionizationfile = sessionizationfile
        self.inactivityperiod = None
        self.wantedFields = ['ip', 'date', 'time', 'cik', 'accession', 'extention']
    
    def read_inactivity(self):
        inactivefh = open(self.inactivityfile, "r")
        for line in inactivefh:
            try:
                self.inactivityperiod = int(line)
            except:
                print ("this is not an integer!")
                continue
            print (line, end="")
        inactivefh.close()

    def read_log(self):
        logfh = open(self.logfile, "r")
        # process on the fly

        header = logfh.readline()
        header = header.strip()
        header = header.split(',')
        print (header)

        for line in logfh.readlines():
            # ip: identifies the IP address of the device requesting the data. While the SEC anonymizes the last three digits, it uses a consistent formula that allows you to assume that any two ip fields with the duplicate values are referring to the same IP address
            # date: date of the request (yyyy-mm-dd)
            # time: time of the request (hh:mm:ss)
            # cik: SEC Central Index Key
            # accession: SEC document accession number
            # extention: Value that helps determine the document being requested
            fa = line.split(',')
            # creates a dictionary with the keys from the header
            faDict = dict(zip(header, fa))
            faDict = {k:v for key in faDict.items() if key in self.wantedFields}
            print (faDict.keys())
        
        logfh.close()


    def write_session(self):
        
        pass

def main():
    parser = argparse.ArgumentParser(description="Edgar Analytics")
    parser.add_argument('log', help="edgar logfile")
    parser.add_argument('inactivity', help="file for inactivity")
    parser.add_argument('sessionization', help="output file with sessions")

    args = parser.parse_args()

    sess = Sessionization(args.log, args.inactivity, args.sessionization)
    sess.read_inactivity()
    sess.read_log()

    # pull args.  test args?
    print ("{} {} {}".format(sess.logfile, sess.inactivityfile, sess.sessionizationfile))

if __name__ == "__main__":
    main()