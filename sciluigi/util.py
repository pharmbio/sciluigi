import time
import os

def timestamp(datefmt='%Y-%m-%d, %H:%M:%S'):
    return time.strftime(datefmt, time.localtime())

def timepath(sep='_'):
    return timestamp('%Y%m%d{sep}%H%M%S'.format(sep=sep))

def timelog():
    return timestamp('[%Y-%m-%d %H:%M:%S]')

def ensuredir(dirpath):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
