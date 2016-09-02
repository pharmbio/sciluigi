'''
This module contains sciluigi's subclasses of luigi's Task class.
'''
import json
import luigi
from luigi.six import iteritems, string_types
import logging
import subprocess as sub
import sciluigi.audit
import sciluigi.interface
import sciluigi.dependencies
import sciluigi.slurm

log = logging.getLogger('sciluigi-interface')

# ==============================================================================

def new_task(name, cls, workflow_task, **kwargs):
    '''
    Instantiate a new task. Not supposed to be used by the end-user
    (use WorkflowTask.new_task() instead).
    '''
    slurminfo = None
    for key, val in [(key, val) for key, val in iteritems(kwargs)]:
        # Handle non-string keys
        if not isinstance(key, string_types):
            raise Exception("Key in kwargs to new_task is not string. Must be string: %s" % key)
        # Handle non-string values
        if isinstance(val, sciluigi.slurm.SlurmInfo):
            slurminfo = val
            kwargs[key] = val
        elif not isinstance(val, string_types):
            try:
                kwargs[key] = json.dumps(val) # Force conversion into string
            except TypeError:
                kwargs[key] = str(val)
    kwargs['instance_name'] = name
    kwargs['workflow_task'] = workflow_task
    kwargs['slurminfo'] = slurminfo
    newtask = cls.from_str_params(kwargs)
    if slurminfo is not None:
        newtask.slurminfo = slurminfo
    return newtask

class Task(sciluigi.audit.AuditTrailHelpers, sciluigi.dependencies.DependencyHelpers, luigi.Task):
    '''
    SciLuigi Task, implementing SciLuigi specific functionality for dependency resolution
    and audit trail logging.
    '''
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()

    def ex_local(self, command):
        '''
        Execute command locally (not through resource manager).
        '''
        # If list, convert to string
        if isinstance(command, list):
            command = sub.list2cmdline(command)

        log.info('Executing command: ' + str(command))
        proc = sub.Popen(command, shell=True, stdout=sub.PIPE, stderr=sub.PIPE)
        stdout, stderr = proc.communicate()
        retcode = proc.returncode

        if len(stderr) > 0:
            log.debug('Stderr from command: %s', stderr)

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
        Execute command. This is a short-hand function, to be overridden e.g. if supporting
        execution via SLURM
        '''
        return self.ex_local(command)

# ==============================================================================

class ExternalTask(
        sciluigi.audit.AuditTrailHelpers,
        sciluigi.dependencies.DependencyHelpers,
        luigi.ExternalTask):
    '''
    SviLuigi specific implementation of luigi.ExternalTask, representing existing
    files.
    '''
    workflow_task = luigi.Parameter()
    instance_name = luigi.Parameter()
