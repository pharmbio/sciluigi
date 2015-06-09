import luigi
import sciluigi
import os

TESTFILE_PATH = '/tmp/test.out'

class TestTask(sciluigi.Task):

    def output(self):
        return sciluigi.create_file_targets(
                out = TESTFILE_PATH)

    def run(self):
        with self.output()['out'].open('w') as outfile:
            outfile.write('File written by luigi\n')

class TestConcatenate2Files():

    def setup(self):
        self.t = TestTask()

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
