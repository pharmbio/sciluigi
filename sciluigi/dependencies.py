import luigi
import time
import random
import string

# ==============================================================================

class TargetInfoParameter(luigi.Parameter):
    pass

# ==============================================================================

# Class to be used for sending specification of which target, from which
# task, to use, when stitching workflow tasks' outputs and inputs together.
class TargetInfo(object):
    task = None
    path = None
    target = None

    def __init__(self, task, path):
        self.task = task
        self.path = path
        self.target = luigi.LocalTarget(path)

    def open(self, *args, **kwargs):
        return self.target.open(*args, **kwargs)

# ==============================================================================

class DependencyHelpers():
    '''
    Mixin implementing methods for supporting dynamic, and target-based
    workflow definition, as opposed to the task-based one in vanilla luigi.
    '''

    # --------------------------------------------------------
    # Handle inputs
    # --------------------------------------------------------

    def requires(self):
        return self._upstream_tasks()

    def _upstream_tasks(self):
        upstream_tasks = []
        for attrname, attrval in self.__dict__.iteritems():
            if callable(attrval) and 'in_' in attrname:
                upstream_tasks.append(attrval().task)
        return upstream_tasks

    # --------------------------------------------------------
    # Handle outputs
    # --------------------------------------------------------

    def output(self):
        return self._output_targets()

    def _output_targets(self):
        outputs = []
        for attrname in dir(self):
            if callable(getattr(self, attrname)) and 'out_' in attrname:
                outputs.append(getattr(self, attrname)().target)
        return outputs
