import os
import shutil
# TODO: Implement!

'''
We want a folder structure something like this:

|-bin
|-conf
|-doc
| \-paper
|-experiments
| \-2000-01-01-example
|   |-audit
|   |-bin
|   |-conf
|   |-data
|   |-doc
|   |-lib
|   |-log
|   |-raw
|   |-results
|   |-run
|   \-tmp
|-lib
|-raw
|-results
\-src
'''

projdir_struct = {
    'bin':None,
    'conf':None,
    'doc' : { 'paper': None },
    'experiments' :
        { '2000-01-01-example' :
            {
                'audit':None,
                'bin':None,
                'conf':None,
                'data':None,
                'doc':None,
                'lib':None,
                'log':None,
                'raw':None,
                'results':None,
                'run':None,
                'tmp':None
            }
        },
    'lib':None,
    'raw':None,
    'results':None,
    'src':None
}

def print_dirs(dir_structure, padding, padstep):
    if type(dir_structure) is dict:
        for k,v in dir_structure.iteritems():
            print str(' ' * padding) + k
            print_dirs(v, padding+padstep, padstep)

def create_dirs(dirtree):
    if type(dirtree) is dict:
        for dir,subtree in dirtree.iteritems():
            print('Creating ' + dir + ' ...')
            os.makedirs(dir)
            if subtree is not None:
              os.chdir(dir)
              create_dirs(subtree)
              os.chdir('..')

create_dirs(projdir_struct)
