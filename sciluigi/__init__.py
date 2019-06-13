'''
Scientific Luigi (SciLuigi for short) is a light-weight wrapper library around
Spotify's Luigi workflow system that aims to make writing scientific workflows
(consisting of numerous interdependent commandline applications) more fluent,
flexible and modular.
'''

from sciluigi import audit
from sciluigi.audit import AuditTrailHelpers

from sciluigi import dependencies
from sciluigi.dependencies import ContainerTargetInfo
from sciluigi.dependencies import TargetInfo
from sciluigi.dependencies import S3TargetInfo
from sciluigi.dependencies import DependencyHelpers

from sciluigi import interface
from sciluigi.interface import run
from sciluigi.interface import run_local
from sciluigi.interface import LOGFMT_STREAM
from sciluigi.interface import LOGFMT_LUIGI
from sciluigi.interface import LOGFMT_SCILUIGI
from sciluigi.interface import DATEFMT

from sciluigi import parameter
from sciluigi.parameter import Parameter

from sciluigi import slurm
from sciluigi.slurm import SlurmInfo
from sciluigi.slurm import SlurmTask
from sciluigi.slurm import SlurmHelpers
from sciluigi.slurm import RUNMODE_LOCAL
from sciluigi.slurm import RUNMODE_HPC
from sciluigi.slurm import RUNMODE_MPI

from sciluigi import task
from sciluigi.task import new_task
from sciluigi.task import Task
from sciluigi.task import ExternalTask
from sciluigi.workflow import WorkflowTask

from sciluigi import util
from sciluigi.util import timestamp
from sciluigi.util import timepath
from sciluigi.util import recordfile_to_dict
from sciluigi.util import dict_to_recordfile

from sciluigi import containertask
from sciluigi.containertask import ContainerInfo
from sciluigi.containertask import ContainerTask
from sciluigi.containertask import ContainerHelpers

from sciluigi.AWSBatchTaskWatcher import AWSBatchTaskWatcher
try:
    batch_task_watcher = AWSBatchTaskWatcher()
except:
    batch_task_watcher = None

import threading

# Lock to ensure only one singularity image is created
singularity_lock = threading.Lock()

def getBatchTaskWatcher():
    global batch_task_watcher
    if batch_task_watcher is None:
        raise NotImplementedError
    return batch_task_watcher
