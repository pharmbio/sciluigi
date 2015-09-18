import luigi
import sciluigi as sl
import time

class T1(sl.Task):

    # Parameter

    text = luigi.Parameter()

    # I/O

    def out_data1(self):
        return sl.TargetInfo(self, 'data/' + self.text + '.txt') # TODO: Of course make the target spec into an object with "get target" method!

    # Implementation

    def run(self):
        time.sleep(1)
        with self.out_data1().open('w') as outfile:
            outfile.write(self.text)

# ========================================================================

class Merge(sl.Task):

    # I/O

    in_data1 = None
    in_data2 = None

    def out_merged(self): 
        return sl.TargetInfo(self, self.in_data1().path + '.merged.txt')

    # Implementation

    def run(self):
        time.sleep(2)
        with self.in_data1().open() as in1, self.in_data2().open() as in2, self.out_merged().open('w') as outfile:

            for row in in1:
                outfile.write(row+'\n')
            for row in in2:
                outfile.write(row+'\n')
