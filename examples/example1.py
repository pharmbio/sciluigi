import logging
import luigi
import sciluigi as sl
from subprocess import call

log = logging.getLogger('sciluigi-interface')

# ------------------------------------------------------------------------
# Workflow class(es)
# ------------------------------------------------------------------------

class MyWorkflow(sl.WorkflowTask):

    def workflow(self):
        rawdata = self.new_task('rawdata', RawData)
        atot = self.new_task('atot', AToT)

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
        cmd = 'cat ' + self.in_data().path + ' | sed "s/A/T/g" > ' + self.out_replatot().path
        log.info("COMMAND TO EXECUTE: " + cmd)
        call(cmd, shell=True)


# Run this file as script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    luigi.run(local_scheduler=True, main_task_cls=MyWorkflow)
