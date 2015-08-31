import logging
import luigi
import sciluigi as sl
import os
import time
import unittest

TESTFILE_PATH = '/tmp/test.out'

log = logging.getLogger('sciluigi-interface')
log.setLevel(logging.WARNING)

class MultiInOutWf(sl.WorkflowTask):
    def workflow(self):
        mo = self.new_task('mout', MultiOutTask)
        mi = self.new_task('min', MultiInTask)
        mi.in_multi = mo.out_multi
        return mi

class MultiOutTask(sl.Task):
    def out_multi(self):
        return [sl.TargetInfo(self, '/tmp/out_%d.txt' % i) for i in xrange(10)]
    def run(self):
        for otgt in self.out_multi():
            with otgt.open('w') as ofile:
                ofile.write('hej')

class MultiInTask(sl.Task):
    in_multi = None
    def out_multi(self):
        return [sl.TargetInfo(self, itgt.path + '.daa.txt') for itgt in self.in_multi()]
    def run(self):
        for itgt, otgt in zip(self.in_multi(), self.out_multi()):
            with itgt.open() as ifile:
                with otgt.open('w') as ofile:
                    ofile.write(ifile.read() + ' daa')

class TestMultiInOutWorkflow(unittest.TestCase):
    def setUp(self):
        self.w = luigi.worker.Worker()

    def test_methods(self):
        wf = sl.WorkflowTask()
        tout = wf.new_task('tout', MultiOutTask)
        tin = wf.new_task('tout', MultiInTask)

        # Assert outputs returns luigi targets, or list of luigi targets
        outs = tout.output()
        self.assertIsInstance(outs, list)
        for out in outs:
            self.assertIsInstance(out, luigi.Target)

        reqs = tin.requires()
        self.assertIsInstance(reqs, list)
        for req in reqs:
            self.assertIsInstance(req, luigi.Task)

    def test_workflow(self):
        wf = MultiInOutWf()
        self.w.add(wf)
        self.w.run()

        # Assert outputs exists
        for p in ['/tmp/out_%d.txt' % i for i in xrange(10)]:
            self.assertTrue(os.path.exists(p))
        for p in ['/tmp/out_%d.txt.daa.txt' % i for i in xrange(10)]:
            self.assertTrue(os.path.exists(p))

        # Remove
        for p in ['/tmp/out_%d.txt' % i for i in xrange(10)]:
            os.remove(p)
        for p in ['/tmp/out_%d.txt.daa.txt' % i for i in xrange(10)]:
            os.remove(p)

    def tearDown(self):
        self.w.stop()

if __name__ == '__main__':
    unittest.main()
