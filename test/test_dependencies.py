import logging
import luigi
import sciluigi as sl
import os
import six.moves as s
import time
import unittest

TESTFILE_PATH = '/tmp/test.out'

log = logging.getLogger('sciluigi-interface')
log.setLevel(logging.WARNING)

class MultiInOutWf(sl.WorkflowTask):
    def workflow(self):
        mo = self.new_task('mout', MultiOutTask, an_id='x')
        mi = self.new_task('min', MultiInTask)
        mi.in_multi = mo.out_multi
        return mi

class MultiOutTask(sl.Task):
    an_id = luigi.Parameter()

    def out_multi(self):
        return [sl.TargetInfo(self, '/tmp/out_%s_%d.txt' % (self.an_id, i)) for i in s.range(10)]
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
        touta = wf.new_task('tout', MultiOutTask,
            an_id='a')
        toutb = wf.new_task('tout', MultiOutTask,
            an_id='b')
        toutc = wf.new_task('tout', MultiOutTask,
            an_id='c')
        tin = wf.new_task('tout', MultiInTask)

        tin.in_multi = [touta.out_multi, {'a': toutb.out_multi, 'b': toutc.out_multi()}]

        # Assert outputs returns luigi targets, or list of luigi targets
        outs = touta.output()
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
        for p in ['/tmp/out_%s_%d.txt' % (aid, i) for i in s.range(10) for aid in ['x']]:
            self.assertTrue(os.path.exists(p))
        for p in ['/tmp/out_%s_%d.txt.daa.txt' % (aid, i) for i in s.range(10) for aid in ['x']]:
            self.assertTrue(os.path.exists(p))

        # Remove
        for p in ['/tmp/out_%s_%d.txt' % (aid, i) for i in s.range(10) for aid in ['x']]:
            os.remove(p)
        for p in ['/tmp/out_%s_%d.txt.daa.txt' % (aid, i) for i in s.range(10) for aid in ['x']]:
            os.remove(p)

    def tearDown(self):
        pass
