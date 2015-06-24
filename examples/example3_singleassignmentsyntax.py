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
        return sciluigi.TargetSpec(self, 'outdata1') # TODO: Of course make the target spec into an object with "get target" method!

    # ------------------------------------------------

    def output(self):
        return { 'outdata1': luigi.LocalTarget('outdata1.txt') }

    def run(self):
        with self.out_data1().resolve().open('w') as outfile:
            outfile.write('hej')

# ========================================================================

class T2(sciluigi.Task):
    in_data1 = None

    def out_data2(self): 
        return sciluigi.TargetSpec(self, 'outdata2')

    # ------------------------------------------------

    def output(self):
        for k,v in self.out().iteritems():
            return { k: luigi.LocalTarget(v) }

    def out(self):
        return { 'outdata2': 'outdata2.txt' }

    def run(self):
        with self.in_data1.resolve().open() as infile:
            with self.out_data2().resolve().open('w') as outfile:
                for row in infile:
                    outfile.write('tjo ' + row + '\n')

# ========================================================================

if __name__ == '__main__':
    luigi.run()
