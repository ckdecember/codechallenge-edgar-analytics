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
                logger.debug(lineDict)

                # short hand var names
                ss = self.session_store
                current_timestamp = "{} {}".format(lineDict['date'], lineDict['time'])
                #key = (lineDict['ip'], current_timestamp)
                key = lineDict['ip']

                logger.info("key is {}".format(key))

                #ss.insert_session(key, lineDict, current_timestamp, self.inactivity_period)
                # if session does not exist, add it, and increment webhit.
                
                # ok sessions can be added now.
                if not ss.session_exists(key):
                    ss.add_session(key, lineDict)
                else:
                    logger.info("outlier key is {}".format(key))
                
                if self.is_valid_session_by_time(key, lineDict, current_timestamp):
                    continue

                # if no match, create a NEW session
                # each line, has a session
                # check that sessions's IP and time
                # if IP matches AND time is within scope, increment web AND update last accessed time aND duration
                # if IP matches and time is NOT within scope, create a NEW session, use preset values in constructor


                

                


            for key in ss.session_dict.keys():
                self.write_user_session(key)

        return
                
    def process_expired(self, key, current_timestamp):
        """ write a user session if the key is expired """
        s = self.session_store.session_dict
        if self.is_expired(key, current_timestamp):
            self.write_user_session(key)
            #del s[key]
        return
    
    def clear_expired_sessions(self, current_timestamp):
        s = self.session_store.session_dict

        for key in list(s.keys()):
            if self.is_expired(key, current_timestamp):
                self.write_user_session(key)
                del s[key]
        return
            
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
        return
    
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
        return
    
    def is_valid_session_by_time(self, key, lineDict, current_timestamp):

        dt_current_timestamp = datetime.strptime(current_timestamp, TIMEFORMAT)

        #logger.info("working now datetime {}".format(dt_current_timestamp))

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
    
    def add_session(self, key, originalRequestDict):
        new_session = session(key, originalRequestDict)
        self.session_dict[key] = new_session
        return
    
    def update_session(self, key, originalRequestDict):
        pass

    def insert_session(self, key, originalRequestDict, current_timestamp, inactivity_period):
        """ insert session, handles if they pre-exist or not! """

        # ok so jfd matches, and properly increments.  the lookback code is wrong

        if self.session_exists(key):
            # this should match perfectly, so add to webcounter.
            logger.info("key existed {}".format(key))
            s = self.session_dict[key]
            s.webrequests += 1
        else:
            logger.info("key should be tuple {}".format(key))
            valid_timestamps = self.get_previous_valid_sessions(current_timestamp, inactivity_period)

            # it always does a lookback.  it's  question of if it's valid or not in exsting keys
            if valid_timestamps:
                logger.info("key is doing lookback {} for {} stamps".format(key, len(valid_timestamps)))

                self.update_back_sessions(key, valid_timestamps)
            else:
                logger.info("key is not doing lookback {}".format(key))
                new_session = session(key, originalRequestDict)
                s.webrequests += 1
                self.session_dict[key] = new_session
        
        for (k,v) in self.session_dict.items():
            logger.info((k,v))
        return

    def get_previous_valid_sessions(self, current_timestamp, inactivity_period) -> list:
        """ look back in the sessions up to the inactivity period for matching sessions """

        dt_current_timestamp = datetime.strptime(current_timestamp, TIMEFORMAT)
        dt_inactivity_period_timedelta = timedelta(seconds=inactivity_period)

        # GOOD SO FAR

        dt_oldest_valid_timestamp = dt_current_timestamp - dt_inactivity_period_timedelta

        dt_valid_timestamps = dt_oldest_valid_timestamp

        delta_step_increment = timedelta(seconds=1)
        valid_timestamps = []

        for i in range(inactivity_period):

            dt_duration = dt_current_timestamp - dt_valid_timestamps 
            dt_valid_timestamps += delta_step_increment
            logger.info("valid_timestamp: {} dt_duration: {} inactivityperiod: {}".format(dt_valid_timestamps, dt_duration, inactivity_period))
            # convert dt_duration into human readable
            
            valid_timestamps.append((dt_valid_timestamps, dt_duration))

        #logger.info("validtimestamps")
        #logger.info(valid_timestamps)
        return valid_timestamps

    def update_back_sessions(self, ip_key, timestamps):
        """ uses older timestamps to update """
        ip = ip_key[0]
        s = self.session_dict

        # check if the keys exist or not
        for (timestamp, duration) in timestamps:
            new_timestamp = timestamp.strftime(TIMEFORMAT)
            back_skey = (ip, new_timestamp)
            logger.info("backkey is {}".format(back_skey))
            if back_skey in s:
                s[b_skey].webrequests += 1
                s[b_skey].duration = duration
                s[b_skey].last_datetime = timestamp
        return
    
class session():
    """ Tracks the user session time access, duration, and page reqs """

    def __init__(self, key, originalRequestDict):
        self.key = key
        self.first_datetime = originalRequestDict['time']
        self.last_datetime = originalRequestDict['time']
        self.duration = 1
        self.webrequests = 1
        self.originalRequestDict = originalRequestDict
        self.delete_flag = False

        fdtstr = "{} {}".format(originalRequestDict['date'], originalRequestDict['time'])
        ldtstr = "{} {}".format(originalRequestDict['date'], originalRequestDict['time'])
        self.dt_first_time = datetime.strptime(fdtstr, TIMEFORMAT)
        self.dt_last_time = datetime.strptime(ldtstr, TIMEFORMAT)

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