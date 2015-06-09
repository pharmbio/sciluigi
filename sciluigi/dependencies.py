import luigi
import time
import random
import string
from collections import namedtuple

# ==============================================================================
# Methods for simplifying creation of (output) targets

def create_file_targets(target_spec=None, **kwargs):
    if len(kwargs) > 0:
        return {name : luigi.LocalTarget(path) for name, path in kwargs.iteritems()}
    else:
        return {name : luigi.LocalTarget(path) for name, path in target_spec.iteritems()}

# ==============================================================================

# Named tuple, used for sending specification of which target, from which
# task, to use, when stitching workflow tasks' outputs and inputs together.
TargetSpec = namedtuple('TargetSpec', ['task', 'output'], rename=True)

# ==============================================================================

class TargetSpecParameter(luigi.Parameter):
    '''
    Parameter whose value is a Target, or actually a TargetSpec
    '''

    def parse(self, s):
        # One could maybe do something more fancy here?
        return s

# ==============================================================================

class DependencyHelpers():
    '''
    Mixin implementing methods for supporting dynamic, and target-based
    workflow definition, as opposed to the task-based one in vanilla luigi.
    '''

    def requires(self):
        return self._upstream_tasks()

    def _upstream_tasks(self):
        upstream_tasks = []
        for param_val in self.param_args:
            if type(param_val) is TargetSpec:
                upstream_tasks.append(param_val[0])
        return upstream_tasks

    # Methods for dynamic wiring of workflow

    def output_spec(self, output_name):
        '''
        Return a specification for an output of a task, to be injected
        into the target-parameters of downstream tasks, whereafter
        the specified task can be obtained by the get_input() method
        of that (downstream task.
        '''
        #return { 'upstream' : { 'task' : self, 'port' : portname } }
        return TargetSpec(task=self, output=output_name)

    def outspec(self, output_name):
        '''
        Short version of output_spec()
        '''
        return self.output_spec(output_name)

    def input(self, input_name):
        '''
        Retrieve the task
        '''
        param = self.param_kwargs[input_name]
        if type(param) is TargetSpec:
            return param[0].output()[param[1]]
        else:
            return param

    def get_path(self, input_name):
        return self.input(input_name).path
