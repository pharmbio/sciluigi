import luigi
import sciluigi
import time

# ========================================================================

class TestWF(sciluigi.WorkflowTask):

    def requires(self):
        t1a = T1(text='hej_hopp')
        t1b = T1(text='hopp_hej')

        mrg1 = Merge()
        mrg2 = Merge()

        print "T1a task id: " + t1a.task_id
        print "T1a hash   : " + str(t1a.__hash__())

        print "T1b task id: " + t1b.task_id
        print "T1b hash   : " + str(t1b.__hash__())

        # Workflow definition
        mrg1.in_data1 = t1a.out_data1()
        mrg1.in_data2 = t1b.out_data1()

        mrg2.in_data1 = t1b.out_data1()
        mrg2.in_data2 = t1a.out_data1()

        print "Mrg1 task id: " + mrg1.task_id
        print "Mrg1 hash   : " + str(mrg1.__hash__())

        print "Mrg2 task id: " + mrg2.task_id
        print "Mrg2 hash   : " + str(mrg2.__hash__())

        return [mrg1, mrg2]

# ========================================================================

class T1(sciluigi.Task):

    text = luigi.Parameter()

    # ------------------------------------------------
    # I/O
    # ------------------------------------------------

    def out_data1(self):
        return sciluigi.TargetInfo(self, self.text + '.txt') # TODO: Of course make the target spec into an object with "get target" method!

    # ------------------------------------------------

    def run(self):
        with self.out_data1().target.open('w') as outfile:
            outfile.write(self.text)

# ========================================================================

class Merge(sciluigi.Task):

    # ------------------------------------------------
    # I/O
    # ------------------------------------------------

    in_data1 = None
    in_data2 = None

    def out_merged(self): 
        return sciluigi.TargetInfo(self, self.in_data1.path + '.merged.txt')

    # ------------------------------------------------

    def run(self):
        with self.in_data1.target.open() as in1, self.in_data2.target.open() as in2, self.out_merged().target.open('w') as outfile:
            for row in in1:
                outfile.write(row+'\n')
            for row in in2:
                outfile.write(row+'\n')

# ========================================================================

if __name__ == '__main__':
    luigi.run(local_scheduler=True)
