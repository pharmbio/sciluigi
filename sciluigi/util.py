'''
This module contains utility methods that are used in various places across the
sciluigi library
'''

import csv
import os
import time

def timestamp(datefmt='%Y-%m-%d, %H:%M:%S'):
    '''
    Create timestamp as a formatted string.
    '''
    return time.strftime(datefmt, time.localtime())

def timepath(sep='_'):
    '''
    Create timestmap, formatted for use in file names.
    '''
    return timestamp('%Y%m%d{sep}%H%M%S'.format(sep=sep))

def timelog():
    '''
    Create time stamp for use in log files.
    '''
    return timestamp('[%Y-%m-%d %H:%M:%S]')

def ensuredir(dirpath):
    '''
    Ensure directory exists.
    '''
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

RECORDFILE_DELIMITER = ':'

def recordfile_to_dict(filehandle):
    '''
    Convert a record file to a dictionary.
    '''
    csvrd = csv.reader(filehandle, delimiter=RECORDFILE_DELIMITER, skipinitialspace=True)
    records = {}
    for row in csvrd:
        records[row[0]] = row[1]
    return records

def dict_to_recordfile(filehandle, records):
    '''
    Convert a dictionary to a recordfile.
    '''
    csvwt = csv.writer(filehandle, delimiter=RECORDFILE_DELIMITER, skipinitialspace=True)
    rows = []
    for key, val in records.iteritems():
        rows.append([key, val])
    csvwt.writerows(rows)
