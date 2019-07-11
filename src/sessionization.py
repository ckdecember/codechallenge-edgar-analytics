"""

Sessionization
take two input files (log and inactivity) and outputs a sessionization file that shows the number of web requests per IP

"""

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
        with open(self.inactivityfile, "r") as inactivefh:
            for line in inactivefh:
                try:
                    self.inactivityperiod = int(line)
                except:
                    logger.debug("This is not an integer!")
                    continue
                #logger.info(line)

    def read_log(self):
        """ Read the CSV log file and append data to the sessionizer file """
        with open(self.logfile, "r") as logfh:
            header = logfh.readline()
            header = header.strip()
            header = header.split(',')
            #logger.debug(header)

            for line in logfh.readlines():
                fa = line.split(',')

                # creates a dictionary with the keys from the header
                faDict = dict(zip(header, fa))
                faDict = {k:v for (k,v) in faDict.items() if k in self.wantedFields}
                #logger.debug(faDict)

                # key maker just for brevity
                key = (faDict['ip'], faDict['date'], faDict['time'])

                if not self.sessionStore.sessionExists(key):
                    self.sessionStore.insertSession(key, faDict)
                else:
                    self.sessionStore.updateSession(key, faDict, self.inactivityperiod)

    def write_session(self):
        # tuple of cik, accession and extention => unique page request
        """ IP address of the user exactly as found in log.csv
        date and time of the first webpage request in the session (yyyy-mm-dd hh:mm:ss)
        date and time of the last webpage request in the session (yyyy-mm-dd hh:mm:ss)
        duration of the session in seconds
        count of webpage requests during the session"""
        #logger.info(self.sessionStore.sessionDict.items())
        for (k,v) in self.sessionStore.sessionDict.items():
            outputStr = "{},{},{},{},{}".format(v.originalRequestDict['ip'], v.firstdatetime, v.lastdatetime, v.duration, v.webrequests)
            logger.info(outputStr)

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
        s.webrequests = s.webrequests + 1
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

        dtCurrentRequestTime = datetime.strptime(currentRequestTime, "%H:%M:%S")
        dtFirstDateTime = datetime.strptime(session.firstdatetime, "%H:%M:%S")

        dtInactivityPeriodDelta = timedelta(seconds=inactivity_period)

        # old, stale session, default it to duration of 2 seconds
        if (dtCurrentRequestTime - dtFirstDateTime)  >= dtInactivityPeriodDelta:
            session.duration = dtInactivityPeriodDelta
            session.webrequests = session.webrequests + 1
            session.lastdatetime = currentRequestTime
        # existing session, mark the duration and increment the count
        else:
            session.duration = dtCurrentRequestTime - dtFirstDateTime
            session.webrequests = session.webrequests + 1
            session.lastdatetime = currentRequestTime
        
        #logger.info(session.duration)

class session():
    """ Tracks the user session time access, duration, and page reqs """

    def __init__(self, key, originalRequestDict):
        self.key = key
        self.firstdatetime = originalRequestDict['time']
        self.lastdatetime = None
        self.duration = 0
        self.listOfWebRequests = []
        self.webrequests = 0
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
    sess.write_session()

    logger.debug("{} {} {}".format(sess.logfile, sess.inactivityfile, sess.sessionizationfile))

if __name__ == "__main__":
    main()