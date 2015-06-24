import luigi
import sciluigi

# ========================================================================

class TestWF(sciluigi.WorkflowTask):

    def requires(self):
        # Task instantiations
        t1a = T1(text='hej\nhopp')
        t1b = T1(text='hej\nhopp')
        mrg = Merge()

        print "T1a task id: " + t1a.task_id
        print "T1b task id: " + t1b.task_id

        # Workflow definition
        mrg.in_data1 = t1a.out_data1()
        mrg.in_data2 = t1b.out_data1()

        # Which task to run up to
        return mrg

# ========================================================================

class T1(sciluigi.Task):

    text = luigi.Parameter()

    # ------------------------------------------------
    # I/O
    # ------------------------------------------------

    def out_data1(self):
        return sciluigi.TargetInfo(self, 'outdata1.txt') # TODO: Of course make the target spec into an object with "get target" method!

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
    luigi.run()
