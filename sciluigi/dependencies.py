import luigi
import time
import random
import string

# ==============================================================================

class TargetInfoParameter(luigi.Parameter):
    pass

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
            if 'in_' == attrname[0:3]:
                if callable(attrval):
                    upstream_tasks.append(attrval().task)
                elif isinstance(attrval, list):
                    upstream_tasks.extend([x().task for x in attrval if callable(x)])
                else:
                    raise Exception('Attribute with name pattern "in_*" was neither callable nor list')
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
