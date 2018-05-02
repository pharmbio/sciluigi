import logging
import luigi
import sciluigi as sl
import os
import time
import unittest

log = logging.getLogger('sciluigi-interface')
log.setLevel(logging.WARNING)


class IntParamTask(sl.Task):
    an_int_param = luigi.IntParameter()

    def out_int_val(self):
        return sl.TargetInfo(self, '/tmp/intparamtask_intval_%d.txt' % self.an_int_param)

    def run(self):
        with self.out_int_val().open('w') as outfile:
            outfile.write('%d' % self.an_int_param)


class NonStringParamWF(sl.WorkflowTask):
    def workflow(self):
        intparam_task = self.new_task('intparam_task', IntParamTask,
                an_int_param = 123)
        return intparam_task


class TestNonStringParameterValues(unittest.TestCase):
    def setUp(self):
        self.w = luigi.worker.Worker()
        self.nsp_wf = NonStringParamWF(instance_name='nonstring_param_wf')
        self.w.add(self.nsp_wf)

    def test_intparam_gets_set(self):
        self.assertEquals(self.nsp_wf._tasks['intparam_task'].an_int_param, 123)

    def test_intparam_gets_set(self):
        self.w.run()
        with self.nsp_wf.workflow().out_int_val().open() as infile:
            val = infile.read()
            self.assertEquals(val, '123')

    def tearDown(self):
        pass
