import luigi
import sciluigi

# ========================================================================

class WF(sciluigi.WorkflowTask):

    def requires(self):
        t1 = T1()
        t2 = T2()
        t2.in_data1 = t1.out_data1()
        return t2

# ========================================================================

class T1(sciluigi.Task):

    def out_data1(self):
        return sciluigi.TargetSpec(self, 'outdata1.txt') # TODO: Of course make the target spec into an object with "get target" method!

    # ------------------------------------------------

    def run(self):
        with self.out_data1().target.open('w') as outfile:
            outfile.write('hej\n')

# ========================================================================

class T2(sciluigi.Task):
    in_data1 = None

    def out_data2(self): 
        return sciluigi.TargetSpec(self, self.in_data1.path + '.outdata2.txt')

    # ------------------------------------------------

    def run(self):
        with self.in_data1.target.open() as infile:
            with self.out_data2().target.open('w') as outfile:
                for row in infile:
                    outfile.write('tjo ' + row + '\n')

# ========================================================================

if __name__ == '__main__':
    luigi.run()
