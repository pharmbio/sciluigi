'''
This module contains functionality for the audit-trail logging functionality
'''

import logging
import luigi
import os
import random
import time

# ==============================================================================

log = logging.getLogger('sciluigi-interface')

# ==============================================================================

class AuditTrailHelpers(object):
    '''
    Mixin for luigi.Task:s, with functionality for writing audit logs of running tasks
    '''
    def add_auditinfo(self, infotype, infoval):
        '''
        Alias to _add_auditinfo(), that can be overridden.
        '''
        return self._add_auditinfo(self.instance_name, infotype, infoval)

    def _add_auditinfo(self, instance_name, infotype, infoval):
        '''
        Save audit information in a designated file, specific for this task.
        '''
        dirpath = self.workflow_task.get_auditdirpath()
        if not os.path.isdir(dirpath):
            time.sleep(random.random())
            if not os.path.isdir(dirpath):
                os.makedirs(dirpath)

        auditfile = os.path.join(dirpath, instance_name)
        if not os.path.exists(auditfile):
            with open(auditfile, 'w') as afile:
                afile.write('[%s]\n' % self.instance_name)
        with open(auditfile, 'a') as afile:
            afile.write('%s: %s\n' % (infotype, infoval))

    def get_instance_name(self):
        '''
        Return the luigi instance_name
        '''
        instance_name = None
        if self.instance_name is not None:
            instance_name = self.instance_name
        else:
            instance_name = self.task_id
        return instance_name

    @luigi.Task.event_handler(luigi.Event.START)
    def save_start_time(self):
        '''
        Log start of execution of task.
        '''
        if hasattr(self, 'workflow_task') and self.workflow_task is not None:
            msg = 'Task {task} started'.format(
                task=self.get_instance_name())
            log.info(msg)

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def save_end_time(self, task_exectime_sec):
        '''
        Log end of execution of task, with execution time.
        '''
        if hasattr(self, 'workflow_task') and self.workflow_task is not None:
            msg = 'Task {task} finished after {proctime:.3f}s'.format(
                task=self.get_instance_name(),
                proctime=task_exectime_sec)
            log.info(msg)
            self.add_auditinfo('task_exectime_sec', '%.3f' % task_exectime_sec)
            for paramname, paramval in self.param_kwargs.iteritems():
                if paramname not in ['workflow_task']:
                    self.add_auditinfo(paramname, paramval)
