import logging
import luigi
import time
import random
import string
from collections import namedtuple
from util import *

# ==============================================================================

log = logging.getLogger('sciluigi-interface')

# ==============================================================================

class AuditTrailHelpers():
    '''
    Mixin for luigi.Task:s, with functionality for writing audit logs of running tasks
    '''
    def add_auditinfo(self, infotype, infoval):
        return self._add_auditinfo(self.instance_name, infotype, infoval)

    def _add_auditinfo(self, instance_name, infotype, infoval):
        dirpath = self.workflow_task.get_auditdirpath()
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        auditfile = os.path.join(dirpath, instance_name)
        if not os.path.exists(auditfile):
            with open(auditfile, 'w') as afile:
                afile.write('[%s]\n' % self.instance_name)
            with open(auditfile, 'a') as afile:
                afile.write('%s: %s\n' % (infotype, infoval))

    def get_instance_name(self):
        instance_name = None
        if self.instance_name is not None:
            instance_name = self.instance_name
        else:
            instance_name = self.task_id
        return instance_name

    def get_timestamp(self):
        return timelog()

    @luigi.Task.event_handler(luigi.Event.START)
    def save_start_time(self):
        if hasattr(self, 'workflow_task') and self.workflow_task is not None:
            msg = 'Task {task} started'.format(
                    task = self.get_instance_name())
            log.info(msg)

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def save_end_time(self, task_exectime_sec):
        if hasattr(self, 'workflow_task') and self.workflow_task is not None:
            msg = 'Task {task} finished after {proctime:.3f}s'.format(
                    task = self.get_instance_name(),
                    proctime = task_exectime_sec)
            log.info(msg)
            self.add_auditinfo('task_exectime_sec', '%.3f' % task_exectime_sec)
