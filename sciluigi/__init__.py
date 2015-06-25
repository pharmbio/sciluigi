from sciluigi import dependencies
from sciluigi import task
from sciluigi import interface
#from sciluigi import slurm
#from sciluigi import audit

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
# TODO: Import ...

# audit module
# TODO: Import ...
