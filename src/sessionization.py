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
import sys

TIMEFORMAT = "%H:%S:%M"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
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
                logger.debug(line)

    def process_log(self):
        """ Read the CSV log file and append data to the sessionizer file """

        self.set_inactivity_period()

        with open(self.logfile, "r") as logfh:
            header = logfh.readline()
            header = header.strip()
            header = header.split(',')
            logger.debug(header)

            for line in logfh.readlines():
                fa = line.split(',')

                # creates a dictionary with the keys from the header
                faDict = dict(zip(header, fa))
                faDict = {k:v for (k,v) in faDict.items() if k in self.wanted_fields}
                logger.debug(faDict)

                # key maker just for brevity
                key = (faDict['ip'])
                ss = self.session_store
                current_timestamp = faDict['time']

                # delete sessions based on global time elapsed
                #self.clear_expired_sessions(current_timestamp)

                if not ss.session_exists(key):
                    ss.insert_session(key, faDict)
                    continue
                
                # did we expire?  if not, keep going.
                # if didn't expire, increment web

                # ok, 5 entries are in there.  since they are unique.  our update code is wrong somehow.
                # update code should
                # update webrequests by 1.  
                # update duration?
                # update lasttime

                sobj = ss.session_dict[key]
                # conditional wrap here.  
                # if currenttimestamp is > firsttimestamp+inactivity
                # make new session (ugh, no wonder why you can't do this.  you made a dict)
                # 
                # check this expiry logic.  it is probably broken.
                #if not self.is_expired(key, current_timestamp):
                #    sobj.webrequests += 1

                """
                # if it exists, 
                sobj = ss.session_dict[key]

                # increment webrequest, adjust duration, change last_datetime
                sobj.webrequests += 1

                sobj.duration = int(self.get_duration(key, current_timestamp).total_seconds())
                sobj.last_datetime = current_timestamp
                
                #self.process_expired(key, faDict['time'])
                #self.session_store.update_session(key)
                """
            
        
        #flush the remainin
        sd = self.session_store.session_dict
        for key in sd.keys():
            self.write_user_session(key)

        logger.info("remaining in session store")
        logger.info(ss.session_dict)
                
    def process_expired(self, key, current_timestamp):
        """ write a user session if the key is expired """
        s = self.session_store.session_dict
        if self.is_expired(key, current_timestamp):
            self.write_user_session(key)
            #del s[key]
    
    def clear_expired_sessions(self, current_timestamp):
        s = self.session_store.session_dict

        for key in list(s.keys()):
            if self.is_expired(key, current_timestamp):
                self.write_user_session(key)
                del s[key]
            
    def is_expired(self, key, current_timestamp):
        """ Tells you if a session has exceeded the inactivity_period """
        dt_duration = self.get_duration(key, current_timestamp)
        dt_inactivity_delta = timedelta(seconds=self.inactivity_period)

        if dt_duration > dt_inactivity_delta:
            logger.debug("duration: {} key: {} timedelta: {} is expired".format(dt_duration, key, dt_inactivity_delta))
            return True
        else:
            return False

    def get_duration(self, key, timestamp):
        """ get timedelta duration between first timestamp and passed timestamp """
        s = self.session_store.session_dict[key]

        dt_current = datetime.strptime(timestamp, TIMEFORMAT)
        dt_first_time = datetime.strptime(s.first_datetime, TIMEFORMAT)

        dt_duration = dt_current - dt_first_time
        logger.debug("key: {} timestamp: {} duration: {}".format(key, timestamp, dt_duration))
        logger.debug("get_duration: dur:{} curr:{} first:{}".format(dt_duration, dt_current, dt_first_time))
        return dt_duration

    def write_user_session(self, key):
        """ writes a session to the sessionalization file """
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
    
    def delete_old_sessions(self):
        s = self.session_store.session_dict
        for key in list(s.keys()):
            if s[key].delete_flag:
                del s[key]

class session_store():
    """ Stores all the discovered sessions """
    def __init__(self):
        self.session_dict = {}
   
    def session_exists(self, key):
        """ check for existing session via key """
        # maybe make it exist by IP
        if key in self.session_dict.keys():
            return True
        else:
            return False
    
    def insert_session(self, key, originalRequestDict):
        """ insert a new session only! """
        # make this beefier
        # session_exists_by_IP
        # if IP matches, consider
        # - incrementing web
        # check if timestamp is ahead or not
        # if timestamp is old, create NEW session
        # 
        s = session(key, originalRequestDict)
        s.originalRequestDict = originalRequestDict
        s.webrequests = s.webrequests + 1
        self.session_dict[key] = s
    
    def update_session(self, key):
        """ increments webcount, updates duration """
        s = self.session_dict[key]
        s.webrequests += 1
        # duration should be current time - first time
        # ok maybe somehow undo the mark?
        logger.debug("{} {}".format(key, s.duration))

class session():
    """ Tracks the user session time access, duration, and page reqs """

    def __init__(self, key, originalRequestDict):
        self.key = key
        self.first_datetime = originalRequestDict['time']
        self.last_datetime = originalRequestDict['time']
        self.duration = 1
        self.webrequests = 0
        self.originalRequestDict = originalRequestDict
        self.delete_flag = False

        self.dt_first_time = datetime.strptime(self.first_datetime, TIMEFORMAT)
        self.dt_last_time = datetime.strptime(self.first_datetime, TIMEFORMAT)

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