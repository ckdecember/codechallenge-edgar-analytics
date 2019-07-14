"""

Sessionization
take two input files (log and inactivity) and outputs a sessionization file that shows the number of web requests per IP

"""

__version__ = '0.1'
__author__ = 'Carroll Kong'

import argparse
from datetime import datetime, timedelta
import logging
import unittest
import os
import sys

TIMEFORMAT = "%Y-%m-%d %H:%M:%S"

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
        self.dt_inactivity_period = None
        self.wanted_fields = ['ip', 'date', 'time', 'cik', 'accession', 'extention']
        self.session_store = session_store()
        self.output_list = []
    
    def set_inactivity_period(self):
        """ get the inactivity period """
        with open(self.inactivity_file, "r") as inactivefh:
            for line in inactivefh:
                try:
                    line = int(line)
                    self.inactivity_period = line
                    self.dt_inactivity_period = timedelta(seconds=line)
                except:
                    logger.debug("This is not an integer!")
                    continue
                logger.debug("inactivity period is {}".format(line))
        return

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
                lineDict = dict(zip(header, fa))
                lineDict = {k:v for (k,v) in lineDict.items() if k in self.wanted_fields}

                # short hand var names
                ss = self.session_store

                current_timestamp = "{} {}".format(lineDict['date'], lineDict['time'])
                dt_current_timestamp = datetime.strptime(current_timestamp, TIMEFORMAT)

                key = lineDict['ip']

                logger.info("key is {}".format(key))

                # first key ever, continue, nothing to check
                if not ss.session_list:
                    logger.info("adding First key {} {}".format(key, lineDict['time']))
                    ss.add_session(key, lineDict, current_timestamp)
                    logger.info("first key - new session list {}".format(ss.session_list))
                    continue

                # checks the existing list and expires old sessions
                self.flush_expired_sessions(dt_current_timestamp)

                # add new key to a non-zero list
                current_session = ss.find_session(key)
                if not current_session:
                    logger.info("adding new key {} {}".format(key, lineDict['time']))
                    ss.add_session(key, lineDict, current_timestamp)
                    logger.info("new session list {}".format(ss.session_list))
                else:
                    ss.update_session(current_session, dt_current_timestamp)

                """
                # look for session, if not found
                current_session = ss.find_session(key)
                # new session
                if not current_session:
                    logger.info("adding new key {} {}".format(key, lineDict['time']))
                    ss.add_session(key, lineDict, current_timestamp)
                    current_session = ss.find_session(key)

                # found a session, if valid by timestamp, update it
                if self.is_valid_session_by_time(dt_current_timestamp, current_session.dt_first_time):
                    logger.info("session is valid by time {} {} {}".format(key, dt_current_timestamp, current_session.dt_first_time))
                    ss.update_session(current_session, dt_current_timestamp)
                else:
                    logger.info("session is NOT valid by time {} {} {}".format(key, dt_current_timestamp, current_session.dt_first_time))
                    # ok, session is expired.  but check ALL old sessions, not just ones matching your timestamp
                    # account for new session with old IP address
                    logger.info("adding new key as final resort {} {}".format(key, lineDict['time']))
                    ss.add_session(key, lineDict, current_timestamp)
                """
                logger.info("####################end key loop####################")
                
        sl = self.session_store.session_list
        for s in sl:
            logger.info("dumping {} from list {}".format(s.key, sl))
            self.write_user_session(s)
        return

    def write_user_session(self, cs):
        """ writes a session to the sessionalization file """
        outputStr = "{},{} {},{},{},{}\n".format(cs.originalRequestDict['ip'], \
            cs.originalRequestDict['date'], cs.first_datetime, \
            cs.last_datetime, int(cs.dt_duration.total_seconds()), cs.webrequests)
        with open(self.sessionization_file, "a") as sfh:
            sfh.write(outputStr)
        return
    
    def clean_stale_output(self):
        try:
            os.rename(self.sessionization_file, self.sessionization_file + ".bak")
        except:
            logger.info("Rename somehow failed...")
        return
    
    def is_valid_session_by_time(self, dt_current, dt_first_time):
        if (dt_current - dt_first_time) > self.dt_inactivity_period:
            return False
        else:
            return True
    
    def flush_expired_sessions(self, dt_current_timestamp):
        """ 
        iterates current session list for expired sessions 
        also writes out the session to the session output
        """
        sl = self.session_store.session_list
        flush_key_list = []
        for s in sl:
            # use get_inclusive
            dt_inclusive_duration = get_inclusive_duration(dt_current_timestamp, s.dt_first_time)
            #if dt_inclusive_duration > self.dt_inactivity_period:
            if dt_inclusive_duration >= self.dt_inactivity_period:
                logger.info("flushing old key {} curtime {} firsttime {} last time {} \
                    duration {}".format(s.key, dt_current_timestamp, \
                        s.dt_first_time, s.dt_last_time, \
                        dt_current_timestamp - s.dt_first_time))
                self.write_user_session(s)
                flush_key_list.append(s.key)
        self.session_store.session_list = [s for s in sl if s.key not in flush_key_list]

class session_store():
    """ Stores all the discovered sessions """
    def __init__(self):
        #self.session_dict = {}
        self.session_list = []
   
    def add_session(self, key, originalRequestDict, current_timestamp):
        """ initialize a new session and append it to the session_list """
        new_session = session(key, originalRequestDict, current_timestamp)
        #self.session_dict[key] = new_session
        self.session_list.append(new_session)
        logger.info("addsession is {} inside list {}".format(new_session, self.session_list))
        return
    
    def find_session(self, key):
        """ find a session by it's session.key and return the session itself """
        #logger.info("key and list {} {}".format(key, self.session_list))
        for i in self.session_list:
            if i.key == key:
                return i
    
    def update_session(self, current_session, dt_current):
        cs = current_session
        cs.webrequests += 1
        cs.dt_duration = get_inclusive_duration(dt_current, cs.dt_first_time)
        #if (cs.dt_duration.total_seconds() == 0):
        #    cs.dt_duration = timedelta(seconds=1)
        cs.dt_last_time = dt_current
        logger.info("updated: key:{} first access {} last access {} duration {} webreq {} "\
            .format(cs.key, cs.dt_first_time, cs.dt_last_time, int(cs.dt_duration.total_seconds()), cs.webrequests,))
        
class session():
    """ Tracks the user session time access, duration, and page reqs """

    def __init__(self, key, originalRequestDict, current_timestamp):
        """ pre initializes """
        self.key = key
        self.first_datetime = originalRequestDict['time']
        self.last_datetime = current_timestamp
        self.duration = 1
        self.webrequests = 1
        self.originalRequestDict = originalRequestDict
        self.delete_flag = False

        fdtstr = "{} {}".format(originalRequestDict['date'], self.first_datetime)
        ldtstr = "{}".format(self.last_datetime)

        self.dt_first_time = datetime.strptime(fdtstr, TIMEFORMAT)
        self.dt_last_time = datetime.strptime(ldtstr, TIMEFORMAT)

        self.dt_duration = timedelta(seconds=self.duration)
    
    def __repr__(self):
        return "{} : {} : {} : {} ".format(self.key, self.first_datetime, \
            self.last_datetime, self.webrequests)

def get_inclusive_duration(t2, t1) -> timedelta:
    """ return a timedelta between t2 and t1 + 1 
        e.g. t2 - t1 + 1 => timedelta
    """
    #dt_inclusive_duration = t2 - t1 + timedelta(seconds=1)
    dt_inclusive_duration = t2 - t1
    return dt_inclusive_duration

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