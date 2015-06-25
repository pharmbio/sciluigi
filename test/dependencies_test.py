import luigi
import sciluigi as sl
import os

TESTFILE_PATH = '/tmp/test.out'

class TestTask(sl.Task):

    def out_data(self):
        return sl.TargetInfo(self, TESTFILE_PATH)

    def run(self):
        with self.out_data().open('w') as outfile:
            outfile.write('File written by luigi\n')

class TestRunTask():

    def setup(self):
        self.t = sl.new_task(self, TestTask, 'testtask')

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
