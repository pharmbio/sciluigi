'''
This module contains functionality for dependency resolution for constructing
the dependency graph of workflows.
'''

import luigi
import warnings
from luigi.contrib.postgres import PostgresTarget
from luigi.contrib.s3 import S3Target

# ==============================================================================

class TargetInfo(object):
    '''
    Class to be used for sending specification of which target, from which
    task, to use, when stitching workflow tasks' outputs and inputs together.
    '''
    task = None
    path = None
    target = None

    def __init__(self, task, path, format=None, is_tmp=False):
        self.task = task
        self.path = path
        self.target = luigi.LocalTarget(path, format, is_tmp)

    def open(self, *args, **kwargs):
        '''
        Forward open method, from luigi's target class
        '''
        return self.target.open(*args, **kwargs)

# ==============================================================================

class S3TargetInfo(TargetInfo):
    def __init__(self, task, path, format=None, client=None):
        self.task = task
        self.path = path
        self.target = S3Target(path, format=format, client=client)

# ==============================================================================

class PostgresTargetInfo(TargetInfo):
    def __init__(self, task, host, database, user, password, update_id, table=None, port=None):
        self.task = task
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.update_id = update_id
        self.table = table
        self.port = port
        self.target = PostgresTarget(host=host, database=database, user=user, password=password, table=table, update_id=update_id, port=port)

# ==============================================================================

class DependencyHelpers(object):
    '''
    Mixin implementing methods for supporting dynamic, and target-based
    workflow definition, as opposed to the task-based one in vanilla luigi.
    '''

    # --------------------------------------------------------
    # Handle inputs
    # --------------------------------------------------------

    def requires(self):
        '''
        Implement luigi API method by returning upstream tasks
        '''
        return self._upstream_tasks()

    def _upstream_tasks(self):
        '''
        Extract upstream tasks from the TargetInfo objects
        or functions returning those (or lists of both the earlier)
        for use in luigi's requires() method.
        '''
        upstream_tasks = set()
        for attrname, attrval in self.__dict__.items():
            if attrname.startswith('in_'):
                upstream_tasks = self._add_upstream_tasks(upstream_tasks, attrval)

        return list(upstream_tasks)

    def _add_upstream_tasks(self, tasks, new_tasks):
        '''
        Recursively loop through lists of TargetInfos, or
        callables returning TargetInfos, or lists of ...
        (repeat recursively) ... and return all tasks.
        '''
        if callable(new_tasks):
            new_tasks = new_tasks()
        if isinstance(new_tasks, TargetInfo):
            tasks.add(new_tasks.task)
        elif isinstance(new_tasks, list):
            for new_task in new_tasks:
                tasks = self._add_upstream_tasks(tasks, new_task)
        elif isinstance(new_tasks, dict):
            for _, new_task in new_tasks.items():
                tasks = self._add_upstream_tasks(tasks, new_task)
        else:
            raise Exception('Input value is neither callable, TargetInfo, nor list: %s' % val)
        return tasks

    # --------------------------------------------------------
    # Handle outputs
    # --------------------------------------------------------

    def output(self):
        '''
        Implement luigi API method
        '''
        return self._output_targets()

    def _output_targets(self):
        '''
        Extract output targets from the TargetInfo objects
        or functions returning those (or lists of both the earlier)
        for use in luigi's output() method.
        '''
        output_targets = []
        for attrname in dir(self):
            with warnings.catch_warnings():
                # Deliberately suppress this deprecation warning, as we are not
                # using the param_args property, only iterating through all
                # members of the class, which triggers the deprecation warning
                # just because of that.
                warnings.filterwarnings('ignore',
                        category=DeprecationWarning,
                        message='Use of param_args has been deprecated')
                attrval = getattr(self, attrname)
                if attrname.startswith('out_'):
                    output_targets = self._parse_outputitem(attrval, output_targets)

        return output_targets

    def _parse_outputitem(self, val, targets):
        '''
        Recursively loop through lists of TargetInfos, or
        callables returning TargetInfos, or lists of ...
        (repeat recursively) ... and return all targets.
        '''
        if callable(val):
            val = val()
        if isinstance(val, TargetInfo):
            targets.append(val.target)
        elif isinstance(val, list):
            for valitem in val:
                targets = self._parse_outputitem(valitem, targets)
        elif isinstance(val, dict):
            for _, valitem in val.items():
                targets = self._parse_outputitem(valitem, targets)
        else:
            raise Exception('Input item is neither callable, TargetInfo, nor list: %s' % val)
        return targets
