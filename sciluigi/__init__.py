from sciluigi import audit
from sciluigi import dependencies
from sciluigi import interface
from sciluigi import parameter
from sciluigi import slurm
from sciluigi import task
from sciluigi import util
#from sciluigi import slurm

# interface
run = interface.run
run_local = interface.run_local

# dependencies module
TargetInfo = dependencies.TargetInfo
TargetInfoParameter = dependencies.TargetInfoParameter
DependencyHelpers = dependencies.DependencyHelpers

# parameter module
Parameter = parameter.Parameter

# task module
new_task = task.new_task
Task = task.Task
ExternalTask = task.ExternalTask
WorkflowTask = task.WorkflowTask

# slurm module
SlurmInfo = slurm.SlurmInfo
SlurmTask = slurm.SlurmTask
SlurmHelpers = slurm.SlurmHelpers
RUNMODE_LOCAL = slurm.RUNMODE_LOCAL
RUNMODE_HPC = slurm.RUNMODE_HPC
RUNMODE_MPI = slurm.RUNMODE_MPI

# audit module
AuditTrailHelpers = audit.AuditTrailHelpers

# Util module
timestamp = util.timestamp
timepath = util.timepath
