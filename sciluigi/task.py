from collections import namedtuple
import luigi
import logging
import random
import string
import subprocess as sub
import time
from sciluigi.util import *
import sciluigi.audit
import sciluigi.interface
import sciluigi.dependencies
import sciluigi.slurm

log = logging.getLogger('sciluigi-interface')

# ==============================================================================

def new_task(name, cls, workflow_task, **kwargs): # TODO: Raise exceptions if params not of right type
    slurminfo = None
    for k, v in [(k,v) for k,v in kwargs.iteritems()]:
        # Handle non-string keys
        if not isinstance(k, basestring):
            raise Exception("Key in kwargs to new_task is not string. Must be string: %s" % k)
        # Handle non-string values
        if isinstance(v, sciluigi.slurm.SlurmInfo):
            slurminfo = v
            kwargs[k] = v
        elif not isinstance(v, basestring):
            kwargs[k] = str(v) # Force conversion into string
    kwargs['instance_name'] = name
    kwargs['workflow_task'] = workflow_task
    kwargs['slurminfo'] = slurminfo
    t = cls.from_str_params(kwargs)
    if slurminfo is not None:
        t.slurminfo = slurminfo
    return t

class Task(sciluigi.audit.AuditTrailHelpers, sciluigi.dependencies.DependencyHelpers, luigi.Task):
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()

    def ex_local(self, command):
        # If list, convert to string
        if isinstance(command, list):
            command = sub.list2cmdline(command)

        log.info('Executing command: ' + str(command))
        proc = sub.Popen(command, shell=True, stdout=sub.PIPE, stderr=sub.PIPE)
        stdout, stderr = proc.communicate()
        retcode = proc.returncode

        if len(stderr) > 0:
            log.debug('Stderr from command: %s' % stderr)

        if retcode != 0:
            errmsg = ('Command failed (retcode {ret}): {cmd}\n'
                      'Command output: {out}\n'
                      'Command stderr: {err}').format(
                    ret=retcode,
                    cmd=command,
                    out=stdout,
                    err=stderr)
            log.error(errmsg)
            raise Exception(errmsg)

        return (retcode, stdout, stderr)

    def ex(self, command):
        '''
        This is a short-hand function, to be overridden e.g. if supporting execution via SLURM
        '''
        return self.ex_local(command)

# ==============================================================================

class ExternalTask(sciluigi.audit.AuditTrailHelpers, sciluigi.dependencies.DependencyHelpers, luigi.ExternalTask):
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()

# ================================================================================

class WorkflowTask(sciluigi.audit.AuditTrailHelpers, luigi.Task):

    instance_name = luigi.Parameter(default='sciluigi_workflow')

    _tasks = {}
    _wfstart = ''
    _wflogpath = ''
    _hasloggedstart = False
    _hasloggedfinish = False
    _hasaddedhandler = False

    def _ensure_timestamp(self):
        if self._wfstart == '':
            self._wfstart = time.strftime('%Y%m%d_%H%M%S', time.localtime())

    def get_wflogpath(self):
        if self._wflogpath == '':
            self._ensure_timestamp()
            clsname = self.__class__.__name__.lower()
            logpath = 'log/workflow_' + clsname + '_started_{t}.log'.format(t=self._wfstart)
            log.info('Logging to %s' % logpath)
            self._wflogpath = logpath
        return self._wflogpath

    def get_auditdirpath(self):
        self._ensure_timestamp()
        clsname = self.__class__.__name__.lower()
        audit_dirpath = 'audit/.audit_%s_%s' % (clsname, self._wfstart)
        return audit_dirpath

    def get_auditlogpath(self):
        self._ensure_timestamp()
        clsname = self.__class__.__name__.lower()
        audit_dirpath = 'audit/workflow_%s_started_%s.audit' % (clsname, self._wfstart)
        return audit_dirpath

    def add_auditinfo(self, infotype, infolog):
        return self._add_auditinfo(self.__class__.__name__.lower(), infotype, infolog)

    def workflow(self):
        raise WorkflowNotImplementedException('workflow() method is not implemented, for ' + str(self))

    def requires(self):
        if not self._hasaddedhandler:
            wflog_formatter = logging.Formatter(sciluigi.interface.logfmt_stream, sciluigi.interface.datefmt)
            wflog_file_handler = logging.FileHandler(self.output()['log'].path)
            wflog_file_handler.setLevel(logging.INFO)
            wflog_file_handler.setFormatter(wflog_formatter)
            log.addHandler(wflog_file_handler)
            luigilog = logging.getLogger('luigi-interface')
            luigilog.addHandler(wflog_file_handler)
            self._hasaddedhandler = True
        clsname = self.__class__.__name__
        if not self._hasloggedstart:
            log.info('-'*80)
            log.info('SciLuigi: %s Workflow Started' % clsname)
            log.info('-'*80)
            self._hasloggedstart = True
        workflow_output = self.workflow()
        if workflow_output is None:
            clsname = self.__class__.__name__
            raise Exception('Nothing returned from workflow() method in the %s Workflow task. Forgot to add a return statement at the end?' % clsname)
        return workflow_output

    def output(self):
        return {'log': luigi.LocalTarget(self.get_wflogpath()),
                'audit': luigi.LocalTarget(self.get_auditlogpath())}

    def run(self):
        if self.output()['audit'].exists():
            errmsg = 'Audit file already exists, when trying to create it: %s' % self.output()['audit'].path
            log.error(errmsg)
            raise Exception(errmsg)
        else:
            with self.output()['audit'].open('w') as auditfile:
                for taskname in sorted(self._tasks):
                    taskaudit_path = os.path.join(self.get_auditdirpath(), taskname)
                    if os.path.exists(taskaudit_path):
                        auditfile.write(open(taskaudit_path).read() + '\n')
        clsname = self.__class__.__name__
        if not self._hasloggedfinish:
            log.info('-'*80)
            log.info('SciLuigi: %s Workflow Finished' % clsname)
            log.info('-'*80)
            self._hasloggedfinish = True

    def new_task(self, instance_name, cls, **kwargs):
        t = new_task(instance_name, cls, self, **kwargs)
        self._tasks[instance_name] = t
        return t

# ================================================================================

class WorkflowNotImplementedException(Exception):
    pass
