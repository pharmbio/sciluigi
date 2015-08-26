import audit
from collections import namedtuple
import commands
import dependencies
import interface
import luigi
import logging
import random
import slurm
import string
import time
from util import *

log = logging.getLogger('sciluigi-interface')

# ==============================================================================

def new_task(name, cls, workflow_task, **kwargs): # TODO: Raise exceptions if params not of right type
    for k, v in [(k,v) for k,v in kwargs.iteritems()]:
        # Handle non-string keys
        if not isinstance(k, basestring):
            raise Exception("Key in kwargs to new_task is not string. Must be string: %s" % k)
        # Handle non-string values
        slurminfo = None
        if isinstance(v, slurm.SlurmInfo):
            slurminfo = v
            kwargs[k] = v
        elif not isinstance(v, basestring):
            kwargs[k] = str(v) # Force conversion into string
    kwargs['instance_name'] = name
    kwargs['workflow_task'] = workflow_task
    t = cls.from_str_params(kwargs)
    if slurminfo is not None:
        t.slurminfo = slurminfo
    return t

class Task(audit.AuditTrailHelpers, dependencies.DependencyHelpers, luigi.Task):
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()

    def ex_local(self, command):
        # If list, convert to string
        if isinstance(command, list):
            command = ' '.join(command)

        log.info('Executing command: ' + str(command))
        (status, output) = commands.getstatusoutput(command) # TODO: Replace with subprocess call!

        # Take care of errors
        if status != 0:
            msg = 'Command failed: {cmd}\nOutput:\n{output}'.format(cmd=command, output=output)
            log.error(msg)
            raise Exception(msg)

        return (status, output)

    def ex(self, command):
        '''
        This is a short-hand function, to be overridden e.g. if supporting execution via SLURM
        '''
        return self.ex_local(command)

# ==============================================================================

class ExternalTask(audit.AuditTrailHelpers, dependencies.DependencyHelpers, luigi.ExternalTask):
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()

# ==============================================================================

class WorkflowTask(audit.AuditTrailHelpers, luigi.Task):

    instance_name = luigi.Parameter(default='sciluigi_workflow')

    _auditinfo = {}
    _wflogpath = ''

    def _get_wflogpath(self):
        if self._wflogpath == '':
            clsname = self.__class__.__name__.lower()
            timestamp = time.strftime('%Y%m%d_%H%M%S', time.localtime())
            self._wflogpath = 'workflow_' + clsname + '_started_{t}.log'.format(t=timestamp)
        return self._wflogpath

    def _add_auditinfo(self, infotype, infoval):
        self.add_auditinfo(self.instance_name, infotype, infoval)

    def add_auditinfo(self, instance_name, infotype, infoval):
        if not instance_name in self._auditinfo:
            self._auditinfo[instance_name] = {}
        self._auditinfo[instance_name][infotype] = infoval

    def workflow(self):
        raise WorkflowNotImplementedException('workflow() method is not implemented, for ' + str(self))

    def requires(self):
        wflog = logging.getLogger('sciluigi-interface')
        wflog_formatter = logging.Formatter('%(asctime)s %(message)s','%Y-%m-%d %H:%M:%S')
        wflog_file_handler = logging.FileHandler(self.output()['log'].path)
        wflog_file_handler.setLevel(logging.INFO)
        wflog_file_handler.setFormatter(wflog_formatter)
        wflog.addHandler(wflog_file_handler)
        return self.workflow()

    def output(self):
        timestamp = time.strftime('%Y%m%d_%H%M%S', time.localtime())
        clsname = self.__class__.__name__.lower()
        auditlogname = 'workflow_' + clsname + '_stopped_{t}.audit'.format(t=timestamp)
        return {'log': luigi.LocalTarget(self._get_wflogpath()),
                'audit': luigi.LocalTarget(auditlogname)}

    def run(self):
        # Write Audit log file
        with self.output()['audit'].open('w') as auditfile:
            for taskname, taskinfo in self._auditinfo.iteritems():
                auditfile.write('\n[%s]\n' % taskname)
                for infotype, infoval in self._auditinfo[taskname].iteritems():
                    auditfile.write('%s: %s\n' % (infotype, infoval))

    def new_task(self, instance_name, cls, **kwargs):
        return new_task(instance_name, cls, self, **kwargs)

    def get_auditinfo(self, instance_name, infotype, infoval):
        if not instance_name in self._auditinfo:
            raise Exception('Audit info not available for task %s' % instance_name)
        if not infotype in self._auditinfo[instance_name]:
            raise Exception('Info %s not available for task %s' % (infotype, instance_name))
        return self._auditinfo[instance_name][infotype]


class WorkflowNotImplementedException(Exception):
    pass
