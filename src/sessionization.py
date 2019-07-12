"""

Sessionization
take two input files (log and inactivity) and outputs a sessionization file that shows the number of web requests per IP
Masha is AWESOME!  I was not paid to write that.

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
        self.outputList = []
    
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
        """ chrono order, so, check if it's past the time, and record/discard """

        self.set_inactivity_period()

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

                # check if session exists,
                # if yes, add another webhit
                # if yes, check if inactivitytime has elapsed.  if so, seal the session.
                # otherwise, keep it alive
                # if nonsession exist, create session.

                # use a sessionClass to store original dict, currenttime, lasttime, duration, webhits
                # cacheList
                # outputList

                # get session data
                # if first session, cache it in a dict, STORE the time too.
                # if not, check current session time vs entire list of cached list.
                # if elements are above inactivity, end their sessions, REMOVE from cachelist.  copy that data in 
                # the 'to be fulfilled' list.  
                # if end of file, go through cachelist, end their sessions.  copy the rest fo fulfil list.
                # run the fulfil list.

                if not self.sessionStore.sessionExists(key):
                    #self.seal_sessions(faDict['time'])
                    self.sessionStore.insertSession(key, faDict)
                else:
                    self.seal_sessions(faDict['time'])
                    # seal old sessions
                    # add to fulfill list (or write immediately)
                    # update session duration, webhits if needed
                    # 
                    self.sessionStore.updateSession(key, faDict, self.inactivityperiod)

            # when we reach the end of the file, seal all existing session

    def seal_sessions(self, timestamp):
        """ check for expired sessions, seal their last time stamps """
        # get current timestamp
        # iterate dictionary, if firsttimestamp
        # convert timestamps
        dtCurrentTimeStamp = datetime.strptime(timestamp, "%H:%M:%S")
        dtInactiveDelta = timedelta(seconds=self.inactivityperiod)
        for (k,v) in self.sessionStore.sessionDict.items():
            dtFirstTimeStamp = datetime.strptime(v.firstdatetime, "%H:%M:%S")
            if (dtCurrentTimeStamp - dtFirstTimeStamp) > dtInactiveDelta:
                v.lastdatetime = v.firstdatetime
                #del self.sessionStore.sessionDict[k]
                self.queue_fulfill(k, v.originalRequestDict)
                v.webrequests = v.webrequests + 1
            #else:
                #v.webrequests = v.webrequests + 1
                #self.queue_fulfill(k, v.originalRequestDict)
        
        for i in self.outputList:
            logger.info(i)

    def queue_fulfill(self, key, session):
        """ queue a session to be sent to output """
        self.outputList.append((key, session))
        #logger.info("are we here")
        #logger.info(self.outputList)

    def write_session(self):
        # tuple of cik, accession and extention => unique page request
        """ IP address of the user exactly as found in log.csv
        date and time of the first webpage request in the session (yyyy-mm-dd hh:mm:ss)
        date and time of the last webpage request in the session (yyyy-mm-dd hh:mm:ss)
        duration of the session in seconds
        count of webpage requests during the session"""
        #logger.info(self.sessionStore.sessionDict.items())

        for (k,v) in self.sessionStore.sessionDict.items():
            outputStr = "{},{} {},{} {},{},{}".format(v.originalRequestDict['ip'], \
                v.originalRequestDict['date'], v.firstdatetime, \
                v.originalRequestDict['date'], v.lastdatetime, v.duration, v.webrequests)
            #logger.info(outputStr)

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

        #logger.info(currentRequestTime)

        dtCurrentRequestTime = datetime.strptime(currentRequestTime, "%H:%M:%S")
        dtFirstDateTime = datetime.strptime(session.firstdatetime, "%H:%M:%S")

        # logger.info(dtCurrentRequestTime)

        dtInactivityPeriodDelta = timedelta(seconds=inactivity_period)

        # old, stale session, default it to duration of 2 seconds
        if (dtCurrentRequestTime - dtFirstDateTime)  > dtInactivityPeriodDelta:
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
    sess.read_log()
    sess.write_session()

    logger.debug("{} {} {}".format(sess.logfile, sess.inactivityfile, sess.sessionizationfile))

if __name__ == "__main__":
    main()