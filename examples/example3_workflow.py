import luigi
import sciluigi as sl
import time
from example3_components import T1, Merge
# ========================================================================

class TestWF(sl.WorkflowTask):

    task = luigi.Parameter()

    def workflow(self):
        t1a = sl.new_task(T1, 't1a', self, text='hej_hopp')
        t1b = sl.new_task(T1, 't1b', self, text='hopp_hej')

        mrg1 = sl.new_task(Merge, 'mrg1', self)
        mrg2 = sl.new_task(Merge, 'mrg2', self)

        # Workflow definition
        mrg1.in_data1 = t1a.out_data1
        mrg1.in_data2 = t1b.out_data1

        mrg2.in_data1 = t1b.out_data1
        mrg2.in_data2 = t1a.out_data1

        for name, instance in locals().iteritems():
            if issubclass(type(instance), sl.Task):
                print '{n} task id: {taskid}\n{n} hash   : {h}'.format(
                        n = name,
                        taskid = instance.task_id,
                        h = instance.__hash__())

        return locals()[self.task]

slurm_conf = {
               'default' :    {
                                 't1a'  : sl.SlurmInfo('core', 1, '1:00:00'),
                                 't1b'  : sl.SlurmInfo('core', 1, '1:00:00'),
                                 'mrg1' : sl.SlurmInfo('core', 1, '1:00:00'),
                                 'mrg2' : sl.SlurmInfo('core', 1, '2:00:00'),
                              },
               # ----------------------------------------------------------------
               'medium' :     {
                                 't1a'  : sl.SlurmInfo('core', 1, '1:00:00'),
                                 't1b'  : sl.SlurmInfo('core', 1, '1:00:00'),
                                 'mrg1' : sl.SlurmInfo('core', 1, '1:00:00'),
                                 'mrg2' : sl.SlurmInfo('core', 1, '2:00:00'),
                              },
               # ----------------------------------------------------------------
               'production' : {
                                 't1a'  : sl.SlurmInfo('core', 1, '1:00:00'),
                                 't1b'  : sl.SlurmInfo('core', 1, '1:00:00'),
                                 'mrg1' : sl.SlurmInfo('core', 1, '1:00:00'),
                                 'mrg2' : sl.SlurmInfo('core', 1, '2:00:00'),
                              },
             }

if __name__ == '__main__':
    sl.run_locally()

