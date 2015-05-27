import luigi
import sciluigi
from subprocess import call

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

class RawData(sciluigi.SciLuigiExternalTask):
    def output(self):
        return { 'rawdata' : luigi.LocalTarget('data/acgt.txt') }

class AToT(sciluigi.SciLuigiTask):
    indata = luigi.Parameter()

    def output(self):
        return { 'atotreplaced' : luigi.LocalTarget(self.get_path('indata') + '.atot') }

    def run(self):
        cmd ='cat ' + self.input('indata').path + ' | sed "s/A/T/g" > ' + self.output()['atotreplaced'].path
        print("COMMAND: " + cmd)
        call(cmd, shell=True)

# ------------------------------------------------------------------------
# Workflow class(es)
# ------------------------------------------------------------------------

class MyWorkflow(luigi.Task):

    def requires(self):

        # Workflow definition
        rawdata = RawData()
        atot = AToT(
                indata=rawdata.out('rawdata')
               )

        return atot

# ------------------------------------------------------------------------
# Run this file as script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    luigi.run()
