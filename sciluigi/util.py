import time

def timestamp(datefmt='%Y-%m-%d, %H:%M:%S'):
    return time.strftime(datefmt, time.localtime())

def timepath(sef='_'):
    return timestamp('%Y%m%d{sep}%H%M%S'.format(sep=sep))

def timelog():
    return timestamp('[%Y-%m-%d %H:%M:%S]')
