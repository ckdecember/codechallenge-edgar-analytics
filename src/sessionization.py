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
        self.sessionStore = sessionStore()
    
    def read_inactivity(self):
        """ get the inactivity period """
        inactivefh = open(self.inactivityfile, "r")
        for line in inactivefh:
            try:
                self.inactivityperiod = int(line)
            except:
                logger.debug("This is not an integer!")
                continue
            print (line, end="")
        inactivefh.close()

    def read_log(self):
        """ Read the CSV log file and append data to the sessionizer file """
        logfh = open(self.logfile, "r")

        header = logfh.readline()
        header = header.strip()
        header = header.split(',')
        logger.debug(header)

        # create a data structure that can temp store the values
        # need a dict of the datastructures for fast access, but could run out of memory
        # identifier for an entry is IP?
        # can only detect 'time' by looking at the next date, so this could get large since you have to temp hold
        # all the values until the end of the file!!!  worry about that later
        # dict with IP, date, time as key?


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
            faDict = {k:v for (k,v) in faDict.items() if k in self.wantedFields}
            logger.debug(faDict)

            # key maker
            key = (faDict['ip'], faDict['date'], faDict['time'])

            if not self.sessionStore.sessionExists(key):
                self.sessionStore.insertSession(key, faDict)
            # look for ip/date/time existance first as key
            # if just IP, there can be dupes.  is that OK?
            # if not, store it in dictOfSessions
            # if yes, compare timestamp in sessionDict compared to one in the current line.
            # if time is longer than the inactivity, seal the session with accurate time or +inactivity_period max.
            # store list of web requests inside 'session' too which is (cik, accession, extention)
            # if webrequest is not in list of webrequests, add to list
            # if it is, don't add to list

        
        logfh.close()

    def write_session(self):
        # tuple of cik, accession and extention => unique page request
        """ IP address of the user exactly as found in log.csv
        date and time of the first webpage request in the session (yyyy-mm-dd hh:mm:ss)
        date and time of the last webpage request in the session (yyyy-mm-dd hh:mm:ss)
        duration of the session in seconds
        count of webpage requests during the session"""

        pass

class sessionStore():
    def __init__(self):
        self.sessionDict = {}
   
    def sessionExists(self, key):
        if key in self.sessionDict.keys():
            return True
        else:
            return False
    
    def insertSession(self, key, sessionDict):
        self.sessionDict[key] = sessionDict

class session():
    """ Tracks the user session time access, duration, and page reqs """
    def __init__(self, ip, datetime):
        self.ip = ip
        self.firstdatetime = datetime
        self.lastdatetime = datetime
        self.duration = None
        self.listOfWebRequests = []

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
    #print ("{} {} {}".format(sess.logfile, sess.inactivityfile, sess.sessionizationfile))
    logger.debug("{} {} {}".format(sess.logfile, sess.inactivityfile, sess.sessionizationfile))

if __name__ == "__main__":
    main()