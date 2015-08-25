import luigi
import audit
from util import *
import dependencies
import time
import random
import slurm
import string
from collections import namedtuple

# ==============================================================================

def new_task(name, cls, workflow_task, **kwargs): # TODO: Raise exceptions if params not of right type
    for k, v in kwargs.iteritems():
        if not isinstance(k, basestring):
            raise Exception("Key in kwargs to new_task is not string. Must be string: %s" % k)
        if not isinstance(v, basestring):
            kwargs[k] = str(v) # Force conversion into string
    kwargs['instance_name'] = name
    kwargs['workflow_task'] = workflow_task
    t = cls.from_str_params(kwargs)
    return t

class Task(audit.AuditTrailHelpers, dependencies.DependencyHelpers, luigi.Task):
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()

# ==============================================================================

class ExternalTask(audit.AuditTrailHelpers, dependencies.DependencyHelpers, luigi.ExternalTask):
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()

# ==============================================================================

class WorkflowTask(audit.AuditTrailHelpers, luigi.Task):

    _auditlog = []

    def workflow(self):
        raise WorkflowNotImplementedException('workflow() method is not implemented, for ' + str(self))

    def requires(self):
        return self.workflow()

    def output(self):
        timestamp = time.strftime('%Y%m%d_%H%M%S', time.localtime())
        clsname = self.__class__.__name__
        return luigi.LocalTarget('workflow_' + clsname.lower() + '_completed_at_{t}.txt'.format(t=timestamp))

    def run(self):
        with self.output().open('w') as outfile:
            outfile.writelines([line + '\n' for line in self._auditlog])
            outfile.write('-'*80 + '\n')
            outfile.write('{time}: {wfname} workflow finished\n'.format(
                            wfname=self.task_family,
                            time=timelog()))

    def new_task(self, instance_name, cls, **kwargs):
        return new_task(instance_name, cls, self, **kwargs)

    def log_audit(self, line):
        self._auditlog.append(line)


class WorkflowNotImplementedException(Exception):
    pass
