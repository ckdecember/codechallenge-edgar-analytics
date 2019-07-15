# codechallenge-edgar-analytics

# Intro
Need to parse EDGAR log files for a few fields.  
Detect user sessions and log them in a sessionization file.

More details can be found here https://github.com/InsightDataScience/edgar-analytics

# General Approach
Use a primary log parser to go line by line
Session Classes to store metadata such as last accessed, webrequests, and a copy of the line itself.
The sessions will be stored in a python list.

The log parser logic will either add to the list, update sessions in the list, or delete from the list.

As sessions are updated or the end of the file, the sessionization file is updated.

# Technology
Just Python 
