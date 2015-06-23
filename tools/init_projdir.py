import os

projdir_struct = {
    'bin':None,
    'conf':None,
    'doc' : 
        { 'paper': None },
    'experiments' :
        { '2000-01-01-example' :
            { 'audit':None,
              'bin':None,
              'conf':None,
              'data':None,
              'doc':None,
              'lib':None,
              'log':None,
              'raw':None,
              'results':None,
              'run':None,
              'tmp':None }},
    'lib':None,
    'raw':None,
    'results':None,
    'src':None }

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

def print_and_create_projdirs():
    print('Now creating the following directory structure:')
    print('-'*80)
    print_dirs(projdir_struct, 0, 2)
    print('-'*80)
    create_dirs(projdir_struct)
    print('-'*80)

if __name__ == '__main__':
    pass
