import luigi
import sciluigi as sl
from subprocess import call
# ------------------------------------------------------------------------
# Workflow class(es)
# ------------------------------------------------------------------------

class MyWorkflow(sl.WorkflowTask):

    def workflow(self):
        rawdata = sl.new_task(RawData)
        atot = sl.new_task(AToT)

        atot.in_data = rawdata.out_rawdata

        return atot

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

class RawData(sl.ExternalTask):

    def out_rawdata(self):
        return sl.TargetInfo(self, 'data/acgt.txt')


class AToT(sl.Task):
    in_data = None

    def out_replatot(self):
        return sl.TargetInfo(self, self.in_data().path + '.atot')

    # ------------------------------------------------

    def run(self):
        cmd ='cat ' + self.in_data().path + ' | sed "s/A/T/g" > ' + self.out_replatot().path
        print("COMMAND: " + cmd)
        call(cmd, shell=True)


# Run this file as script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    luigi.run(local_scheduler=True)

