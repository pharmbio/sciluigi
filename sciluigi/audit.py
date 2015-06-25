import luigi
import time
import random
import string
from collections import namedtuple
from util import *

# ==============================================================================

class AuditTrailHelpers():
    '''
    Mixin for luigi.Task:s, with functionality for writing audit logs of running tasks
    '''
    def get_task_name(self):
        task_name = None
        if self.task_name is not None:
            task_name = self.task_name
        else:
            task_name = self.task_id
        return task_name

    def get_timestamp(self):
        return timelog()

    @luigi.Task.event_handler(luigi.Event.START)
    def save_start_time(self):
        if hasattr(self, 'workflow_task'):
            msg = '{time}: {task} started'.format(
                    time = self.get_timestamp(),
                    task = self.get_task_name())
            self.workflow_task.log_audit(msg)

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def save_end_time(self, task_exectime_sec):
        if hasattr(self, 'workflow_task'):
            msg = '{time}: {task} finished with processing time: {proctime:.9f} s'.format(
                    time = self.get_timestamp(),
                    task = self.get_task_name(),
                    proctime = task_exectime_sec)
            self.workflow_task.log_audit(msg)
