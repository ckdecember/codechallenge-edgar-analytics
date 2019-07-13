"""

Sessionization
take two input files (log and inactivity) and outputs a sessionization file that shows the number of web requests per IP
Masha is AWESOME!  I was not paid to write that.

"""

__version__ = '0.1'
__author__ = 'Carroll Kong'

import argparse
from datetime import datetime, timedelta
import logging
import unittest
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch.setFormatter(formatter)
logger.addHandler(ch)

class Sessionization:
    def __init__(self, logfile, inactivity_file, sessionization_file):
        self.logfile = logfile
        self.inactivity_file = inactivity_file
        self.sessionization_file = sessionization_file
        self.inactivity_period = None
        self.wanted_fields = ['ip', 'date', 'time', 'cik', 'accession', 'extention']
        self.session_store = session_store()
        self.output_list = []
    
    def set_inactivity_period(self):
        """ get the inactivity period """
        with open(self.inactivity_file, "r") as inactivefh:
            for line in inactivefh:
                try:
                    self.inactivity_period = int(line)
                except:
                    logger.debug("This is not an integer!")
                    continue
                #logger.info(line)

    def process_log(self):
        """ Read the CSV log file and append data to the sessionizer file """

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
                faDict = {k:v for (k,v) in faDict.items() if k in self.wanted_fields}
                #logger.debug(faDict)

                # key maker just for brevity
                key = (faDict['ip'], faDict['date'], faDict['time'])

                # if not, check current session time vs entire list of cached list.
                # if elements are above inactivity, end their sessions, REMOVE from cachelist.  copy that data in 
                # the 'to be fulfilled' list.  
                # if end of file, go through cachelist, end their sessions.  copy the rest fo fulfil list.
                # run the fulfil list.

                self.mark_expired(faDict['time'])

                if not self.session_store.session_exists(key):
                    self.session_store.insert_session(key, faDict)
                else:
                    self.session_store.update_session(key)
                
                self.flush_user_session(key)
                
                # queue to outputlist if marked
                # delete from sessionlist

            # when we reach the end of the file, seal all existing session

    def mark_expired(self, current_timestamp):
        """ 
        Loops through cached session to mark expired sessions
        also adds to the outputList
        maybe just mark, and have another one add to the outputlist?
        """
        for (k,v) in self.session_store.session_dict.items():
            if self.is_expired(k, current_timestamp):
                v.delete_flag = 1
            
    def is_expired(self, key, current_timestamp):
        """ Tells you if it has exceeded the inactivity_period """
        first_timestamp = self.session_store.session_dict[key].first_datetime
        dt_first = self.dt_timestamper(first_timestamp)
        dt_current = self.dt_timestamper(current_timestamp)
        dt_inactivity_delta = timedelta(seconds=self.inactivity_period)

        if (dt_current - dt_first) > dt_inactivity_delta:
            logger.info("{} is expired".format(key))
            return True
        else:
            return False

    def dt_timestamper(self, timestamp):
        dtTimeStamp = datetime.strptime(timestamp, "%H:%M:%S")
        return dtTimeStamp

    def flush_user_session(self, key):
        """ queue a session to be sent to output """
        s = self.session_store.session_dict[key]
        outputStr = "{},{} {},{} {},{},{}\n".format(s.originalRequestDict['ip'], \
            s.originalRequestDict['date'], s.first_datetime, \
            s.originalRequestDict['date'], s.last_datetime, s.duration, s.webrequests)
        with open(self.sessionization_file, "a") as sfh:
            sfh.write(outputStr)
    
    def clean_stale_output(self):
        try:
            os.rename(self.sessionization_file, self.sessionization_file + ".bak")
        except:
            logger.info("Rename somehow failed...")
        return

class session_store():
    """ Stores all the discovered sessions """
    def __init__(self):
        self.session_dict = {}
   
    def session_exists(self, key):
        """ check for existing session via key """
        if key in self.session_dict.keys():
            return True
        else:
            return False
    
    def insert_session(self, key, originalRequestDict):
        """ insert a new session only! """
        s = session(key, originalRequestDict)
        s.originalRequestDict = originalRequestDict
        s.webrequests = s.webrequests + 1
        self.session_dict[key] = s
    
    def update_session(self, key):
        """ increments webcount """
        self.session_dict[key].webrequests += 1
        #logger.info(session.duration)

class session():
    """ Tracks the user session time access, duration, and page reqs """

    def __init__(self, key, originalRequestDict):
        self.key = key
        self.first_datetime = originalRequestDict['time']
        self.last_datetime = originalRequestDict['time']
        self.duration = 0
        self.webrequests = 0
        self.originalRequestDict = originalRequestDict
        self.delete_flag = False

def main():
    parser = argparse.ArgumentParser(description="Edgar Analytics")
    parser.add_argument('log', help="edgar logfile")
    parser.add_argument('inactivity', help="file for inactivity")
    parser.add_argument('sessionization', help="output file with sessions")

    args = parser.parse_args()

    sess = Sessionization(args.log, args.inactivity, args.sessionization)
    sess.clean_stale_output()
    sess.process_log()

    logger.debug("{} {} {}".format(sess.logfile, sess.inactivity_file, sess.sessionization_file))

if __name__ == "__main__":
    main()