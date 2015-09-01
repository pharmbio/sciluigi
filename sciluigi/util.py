import csv
import os
import time

def timestamp(datefmt='%Y-%m-%d, %H:%M:%S'):
    return time.strftime(datefmt, time.localtime())

def timepath(sep='_'):
    return timestamp('%Y%m%d{sep}%H%M%S'.format(sep=sep))

def timelog():
    return timestamp('[%Y-%m-%d %H:%M:%S]')

def ensuredir(dirpath):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

RECORDFILE_DELIMITER = ':'

def recordfile_to_dict(filehandle):
    csvrd = csv.reader(filehandle, delimiter=RECORDFILE_DELIMITER, skipinitialspace=True)
    records = {}
    for row in csvrd:
        records[row[0]] = row[1]
    return records

def dict_to_recordfile(filehandle, records):
    csvwt = csv.writer(filehandle, delimiter=RECORDFILE_DELIMITER, skipinitialspace=True)
    rows = []
    for k, v in records.iteritems():
       rows.append([k, v])
    csvwt.writerows(rows)
