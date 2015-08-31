import logging
import luigi
import sciluigi as sl
import os

TESTFILE_PATH = '/tmp/test.out'

log = logging.getLogger('sciluigi-interface')
log.setLevel(logging.WARNING)

class TestTask(sl.Task):

    def out_data(self):
        return sl.TargetInfo(self, TESTFILE_PATH)

    def run(self):
        with self.out_data().open('w') as outfile:
            outfile.write('File written by luigi\n')

class TestRunTask():

    def setup(self):
        wf = sl.WorkflowTask()
        self.t = sl.new_task('testtask', TestTask, wf)

    def teardown(self):
        self.t = None
        os.remove(TESTFILE_PATH)

    def test_run(self):
        # Run a task with a luigi worker
        w = luigi.worker.Worker()
        w.add(self.t)
        w.run()
        w.stop()

        assert os.path.isfile(TESTFILE_PATH)

class TestMultiOut():
    wf = None
    def setup(self):
        self.wf = MultiOutWf()
    def teardown(self):
        del(self.wf)
    def test_run(self):
        self.wf.run()

class MultiOutWf(sl.WorkflowTask):
    def workflow(self):
        m_out = self.new_task('mout', MultiOutTask)
        m_in = self.new_task('min', MultiInTask)
        m_in.in_multi = m_out.out_multi
        return m_in

class MultiOutTask(sl.Task):
    def out_multi(self):
        return [sl.TargetInfo(self, 'out_%d.txt' % i) for i in xrange(10)]
    def run(self):
        for otgtg in out_multi():
            with otgt.open('w') as ofile:
                ofile.write('hej')

class MultiInTask(sl.Task):
    in_multi = None
    def out_multi(self):
        return [sl.TargetInfo(self, itgt.path + '.daa') for itgt in self.in_multi()]
    def run(self):
        for itgt, otgt in zip(in_multi(), out_multi()):
            with itgt.open() as ifile:
                with otgt.open('w') as ofile:
                    ofile.write(ifile.read() + ' daa')

#if __name__ == '__main__':
#    sl.run_local(main_task_cls=MultiOutWf)
