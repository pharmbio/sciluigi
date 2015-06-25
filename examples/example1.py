import luigi
import sciluigi as sl
from subprocess import call
# ------------------------------------------------------------------------
# Workflow class(es)
# ------------------------------------------------------------------------

class MyWorkflow(sl.WorkflowTask):

    def workflow(self):
        rawdata = sl.new_task(RawData)
        atot = sl.new_task(ATOT)

        atot.in_

        #        indata=rawdata.outspec('rawdata')
        
        return atot

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

class RawData(sciluigi.ExternalTask):
    def output(self):
        return { 'rawdata' : luigi.LocalTarget('data/acgt.txt') }

class AToT(sciluigi.Task):
    in_data = None

    def output(self):
        return { 'atotreplaced' : luigi.LocalTarget(self.get_path('indata') + '.atot') }

    def run(self):
        cmd ='cat ' + self.input('indata').path + ' | sed "s/A/T/g" > ' + self.output()['atotreplaced'].path
        print("COMMAND: " + cmd)
        call(cmd, shell=True)


# Run this file as script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    luigi.run()
