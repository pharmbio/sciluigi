import luigi
import time
import random
import string
from collections import namedtuple

# ==============================================================================

class AuditTrailHelpers():
    '''
    Mixin for luigi.Task:s, with functionality for writing audit logs of running tasks
    '''
    start_time = None
    end_time = None
    exec_time = None

    replicate_id = luigi.Parameter()

    @luigi.Task.event_handler(luigi.Event.START)
    def save_start_time(self):
        task_name = self.task_id.split('(')[0]
        unique_id = time.strftime('%Y%m%d.%H%M%S', time.localtime()) + '.' + ''.join(random.choice(string.ascii_lowercase) for _ in range(3))
        base_dir = 'audit/{replicate_id}'.format(
                    replicate_id=self.replicate_id
                )
        self.auditlog_file = '{base_dir}/{dataset_name}.{task_name}.{unique_id}.audit.log'.format(
                    base_dir=base_dir,
                    dataset_name=self.dataset_name,
                    task_name=task_name,
                    unique_id=unique_id
                )
        self.start_time = time.time()
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        with open(self.auditlog_file, 'w') as log:
            tsv_writer = csv.writer(log, delimiter='\t')
            # Write the value of all the tasks variables to the audit log
            for k, v in self.__dict__.iteritems():
                tsv_writer.writerow([k, v])

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def save_end_time(self, task_exectime_sec):
        self.end_time = time.time()
        if hasattr(self, 'slurm_exectime_sec'):
            self.slurm_queuetime_sec = int(task_exectime_sec) - int(self.slurm_exectime_sec)
        if hasattr(self, 'auditlog_file'):
            with open(self.auditlog_file, 'a') as log:
                tsv_writer = csv.writer(log, delimiter='\t')
                tsv_writer.writerow(['end_time', int(self.end_time)])
                if hasattr(self, 'slurm_exectime_sec'):
                    tsv_writer.writerow(['slurm_queuetime_sec', int(self.slurm_queuetime_sec)])
                    tsv_writer.writerow(['total_tasktime_sec', int(task_exectime_sec)])
                    tsv_writer.writerow(['derived_runtime_sec', int(self.slurm_exectime_sec)])
                else:
                    tsv_writer.writerow(['total_tasktime_sec', int(task_exectime_sec)])
                    tsv_writer.writerow(['derived_runtime_sec', int(task_exectime_sec)])
        else:
            log.info("No audit_log set, so not writing audit log for " + str(self))
