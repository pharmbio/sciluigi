import luigi
import dependencies
import time
import random
import string
from collections import namedtuple

# ==============================================================================

class Task(dependencies.DependencyHelpers, luigi.Task):
    sid = luigi.Parameter(default=None)

#    def __init__(self, *args, **kwargs):
#        '''
#        Adding a custom random id, to ensure uniqueness of tasks that would
#        otherwise not seem unique
#        '''
#        kwargs['sid'] = str(random.random())[2:]
#        super(Task, self).__init__(*args, **kwargs)

# ==============================================================================

class ExternalTask(dependencies.DependencyHelpers, luigi.ExternalTask):
    pass

# ==============================================================================

class WorkflowTask(luigi.Task):

    def output(self):
        timestamp = time.strftime('%Y%m%d_%H%M%S', time.localtime())
        clsname = self.__class__.__name__
        return luigi.LocalTarget('workflow_' + clsname.lower() + '_completed_at_{t}'.format(t=timestamp))

    def run(self):
        timestamp = time.strftime('%Y-%m-%d, %H:%M:%S', time.localtime())
        with self.output().open('w') as outfile:
            outfile.write('workflow finished at {t}'.format(t=timestamp))
