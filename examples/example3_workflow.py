import luigi
import sciluigi as sl
import time
from example3_components import T1, Merge
# ========================================================================

class TestWF(sl.WorkflowTask):

    task = luigi.Parameter()

    def workflow(self):
        t1a = sl.new_task(self, T1, 't1a', text='hej_hopp')
        t1b = sl.new_task(self, T1, 't1b', text='hopp_hej')

        mrg1 = sl.new_task(self, Merge, 'mrg1')
        mrg2 = sl.new_task(self, Merge, 'mrg2')

        # Workflow definition
        mrg1.in_data1 = t1a.out_data1
        mrg1.in_data2 = t1b.out_data1

        mrg2.in_data1 = t1b.out_data1
        mrg2.in_data2 = t1a.out_data1

        print "T1a task id: " + t1a.task_id
        print "T1a hash   : " + str(t1a.__hash__())

        print "T1b task id: " + t1b.task_id
        print "T1b hash   : " + str(t1b.__hash__())

        print "Mrg1 task id: " + mrg1.task_id
        print "Mrg1 hash   : " + str(mrg1.__hash__())

        print "Mrg2 task id: " + mrg2.task_id
        print "Mrg2 hash   : " + str(mrg2.__hash__())

        return locals()[self.task]

if __name__ == '__main__':
    sl.run_locally()
