import argparse
from datetime import datetime, timedelta
import logging
import unittest

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch.setFormatter(formatter)
logger.addHandler(ch)

class Sessionization:
    def __init__(self, logfile, inactivityfile, sessionizationfile):
        self.logfile = logfile
        self.inactivityfile = inactivityfile
        self.sessionizationfile = sessionizationfile
        self.inactivityperiod = None
        self.wantedFields = ['ip', 'date', 'time', 'cik', 'accession', 'extention']
        self.sessionStore = sessionStore()
    
    def set_inactivity_period(self):
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

            # key maker just for brevity
            key = (faDict['ip'], faDict['date'], faDict['time'])

            if not self.sessionStore.sessionExists(key):
                self.sessionStore.insertSession(key, faDict)
            else:
                self.sessionStore.updateSession(key, faDict, self.inactivityperiod)
            
            # look for ip/date/time existance first as key
            # if not, store it in dictOfSessions
            # if yes, compare timestamp in sessionDict compared to one in the current line.
            # if time is longer than the inactivity, seal the session with accurate time or +inactivity_period max.
            # store list of web requests inside 'session' too which is (cik, accession, extention)
            # if webrequest is not in list of webrequests, add to list
            # if it is, don't add to list

            # maybe store the value as a session object which also contains original
        
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
    """ Stores all the discovered sessions """
    def __init__(self):
        self.sessionDict = {}
   
    def sessionExists(self, key):
        """ check for existing session via key """
        if key in self.sessionDict.keys():
            return True
        else:
            return False
    
    def insertSession(self, key, originalRequestDict):
        """ insert a new session only! """
        s = session(key, originalRequestDict)
        s.originalRequestDict = originalRequestDict
        self.sessionDict[key] = s
    
    def updateSession(self, key, originalRequestDict, inactivity_period):
        """ update existing session """
        # if yes, compare timestamp in sessionDict compared to one in the current line.
        # if time is longer than the inactivity, seal the session with accurate time or +inactivity_period max.
        # store list of web requests inside 'session' too which is (cik, accession, extention)
        # if webrequest is not in list of webrequests, add to list
        # if it is, don't add to list
        currentRequestTime = originalRequestDict['time']
        session = self.sessionDict[key]

        # compare times
        # if it's longer than the period or equal the session ends. count duration.
        #dateobj = datetime.datetime(str(currentRequestTime))
        logger.info("currentreqtimef")
        #timeobj = datetime(currentRequestTime)
        #logger.info(timeobj)

        currentRequestTime = datetime.strptime(currentRequestTime, "%H:%M:%S")
        firstdatetime = datetime.strptime(session.firstdatetime, "%H:%M:%S")

        inactivity_period_delta = timedelta(seconds=inactivity_period)

        logger.info(currentRequestTime)
        logger.info(firstdatetime)

        if (currentRequestTime - firstdatetime)  >= inactivity_period_delta:
            logger.info("in!")
            # just add inactivity period and ignore the rest
        #    session.duration = session.firstdatetime + inactivity_period
        #else:
        #    session.duration = currentRequestTime - session.firstdatetime

class session():
    """ Tracks the user session time access, duration, and page reqs """

    def __init__(self, key, originalRequestDict):
        self.key = key
        self.firstdatetime = originalRequestDict['time']
        self.lastdatetime = None
        self.duration = 0
        self.listOfWebRequests = []
        self.originalRequestDict = originalRequestDict

def main():
    parser = argparse.ArgumentParser(description="Edgar Analytics")
    parser.add_argument('log', help="edgar logfile")
    parser.add_argument('inactivity', help="file for inactivity")
    parser.add_argument('sessionization', help="output file with sessions")

    args = parser.parse_args()

    sess = Sessionization(args.log, args.inactivity, args.sessionization)
    sess.set_inactivity_period()
    sess.read_log()

    logger.debug("{} {} {}".format(sess.logfile, sess.inactivityfile, sess.sessionizationfile))

if __name__ == "__main__":
    main()