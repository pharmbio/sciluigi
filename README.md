Scientific Luigi
================

*Note: This is work in progress, with a rapidly changing API! (See feature branches for new functionality and APIs being developed). API expected to stabilize during summer 2015.*

Extra helper methods for writing scientific workflows in [Luigi](http://github.com/spotify/luigi).

More info is coming, but this library enables to define luigi tasks and workflows
in the following way:

```python
import luigi
import sciluigi
import math
from subprocess import call
import subprocess as sub
import sys
import requests
import time

# ------------------------------------------------------------------------
# Workflow class
# ------------------------------------------------------------------------

class MyWorkflow(sciluigi.WorkflowTask):

    task = luigi.Parameter()

    def requires(self):

		# ----------------------------------------------------------------
        # Workflow definition goes below!
		# ----------------------------------------------------------------

        tasks = {}
        tasks['rawdata'] = ExistingData()

        # Split a file
        tasks['split'] = SplitAFile(
                indata = tasks['rawdata'].outspec('acgt'))

        # Run the same task on the two splits
        tasks['dosth1'] = DoSomething(
                indata = tasks['split'].outspec('part1'))
        tasks['dosth2'] = DoSomething(
                indata = tasks['split'].outspec('part2'))

        # Merge the results
        tasks['merge'] = MergeFiles(
                part1 = tasks['dosth1'].outspec('outdata'),
                part2 = tasks['dosth2'].outspec('outdata'))

		# Make the task to return selectable, with the self.task parameter
        return tasks[self.task]

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

# Split a file
class ExistingData(sciluigi.ExternalTask):
    file_name = luigi.Parameter(default='acgt.txt')

    def output(self):
        return sciluigi.create_file_targets(
             acgt = 'data/' + self.file_name)

class SplitAFile(sciluigi.Task):
    indata = sciluigi.TargetSpecParameter()

    def output(self):
        return sciluigi.create_file_targets(
            part1 = self.input('indata').path + '.part1',
            part2 = self.input('indata').path + '.part2')


    def run(self):
        cmd = 'wc -l {f}'.format(f=self.get_path('indata') )
        wc_output = sub.check_output(cmd, shell=True)
        lines_cnt = int(wc_output.split(' ')[0])
        head_cnt = int(math.ceil(lines_cnt / 2))
        tail_cnt = int(math.floor(lines_cnt / 2))

        cmd_head = 'head -n {cnt} {i} > {part1}'.format(
            i=self.get_path('indata'),
            cnt=head_cnt,
            part1=self.output()['part1'].path)
        print("COMMAND: " + cmd_head)
        sub.call(cmd_head, shell=True)

        sub.call('tail -n {cnt} {i} {cnt} > {part2}'.format(
            i=self.get_path('indata'),
            cnt=tail_cnt,
            part2=self.output()['part2'].path),
        shell=True)


# Run the same program on both parts of the split
class DoSomething(sciluigi.Task):
    indata = sciluigi.TargetSpecParameter()

    def output(self):
        return sciluigi.create_file_targets(
            outdata = self.get_path('indata') + '.something_done')

    def run(self):
        with self.input('indata').open() as infile, self.output()['outdata'].open('w') as outfile:
            for line in infile:
                outfile.write(line.lower() + '\n')


# Merge the results of the programs
class MergeFiles(sciluigi.Task):
    part1 = sciluigi.TargetSpecParameter()
    part2 = sciluigi.TargetSpecParameter()

    def output(self):
        return sciluigi.create_file_targets(
            merged = self.input('part1').path + '.merged'
        )

    def run(self):
        sub.call('cat {f1} {f2} > {out}'.format(
            f1=self.input('part1').path,
            f2=self.input('part2').path,
            out=self.output()['merged'].path),
        shell=True)

# ------------------------------------------------------------------------
# Run this file as a script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    luigi.run()
```

Then you would run this as:

```bash
python example.py --local-scheduler MyWorkflow --task merge
```
