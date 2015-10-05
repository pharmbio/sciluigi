'''
This module contains functionality for dependency resolution for constructing
the dependency graph of workflows.
'''

import luigi

# ==============================================================================

class TargetInfo(object):
    '''
    Class to be used for sending specification of which target, from which
    task, to use, when stitching workflow tasks' outputs and inputs together.
    '''
    task = None
    path = None
    target = None

    def __init__(self, task, path):
        self.task = task
        self.path = path
        self.target = luigi.LocalTarget(path)

    def open(self, *args, **kwargs):
        '''
        Forward open method, from luigi's target class
        '''
        return self.target.open(*args, **kwargs)

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
        upstream_tasks = []
        for attrname, attrval in self.__dict__.iteritems():
            if 'in_' == attrname[0:3]:
                upstream_tasks = self._parse_inputitem(attrval, upstream_tasks)

        return upstream_tasks

    def _parse_inputitem(self, val, tasks):
        '''
        Recursively loop through lists of TargetInfos, or
        callables returning TargetInfos, or lists of ...
        (repeat recursively) ... and return all tasks.
        '''
        if callable(val):
            val = val()
        if isinstance(val, TargetInfo):
            tasks.append(val.task)
        elif isinstance(val, list):
            for valitem in val:
                tasks = self._parse_inputitem(valitem, tasks)
        elif isinstance(val, dict):
            for _, valitem in val.iteritems():
                tasks = self._parse_inputitem(valitem, tasks)
        else:
            raise Exception('Input item is neither callable, TargetInfo, nor list: %s' % val)
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
            attrval = getattr(self, attrname)
            if attrname[0:4] == 'out_':
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
            for _, valitem in val.iteritems():
                targets = self._parse_outputitem(valitem, targets)
        else:
            raise Exception('Input item is neither callable, TargetInfo, nor list: %s' % val)
        return targets
