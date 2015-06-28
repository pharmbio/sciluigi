from sciluigi import audit
from sciluigi import dependencies
from sciluigi import interface
from sciluigi import slurm
from sciluigi import task
from sciluigi import util
#from sciluigi import slurm

# interface
run = interface.run
run_locally = interface.run_locally

# dependencies module
TargetInfo = dependencies.TargetInfo
TargetInfoParameter = dependencies.TargetInfoParameter
DependencyHelpers = dependencies.DependencyHelpers

# task module
new_task = task.new_task
Task = task.Task
ExternalTask = task.ExternalTask
WorkflowTask = task.WorkflowTask

# slurm module
SlurmInfo = slurm.SlurmInfo

# audit module
AuditTrailHelpers = audit.AuditTrailHelpers

# Util module
timestamp = util.timestamp
timepath = util.timepath
