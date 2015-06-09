import luigi
import dependencies
import time
import random
import string
from collections import namedtuple

# ==============================================================================

class Task(dependencies.DependencyHelpers, luigi.Task):
    pass

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
