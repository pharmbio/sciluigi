Scientific Luigi
================

Extra helper methods for writing scientific workflows in [Luigi](http://github.com/spotify/luigi).

More info is coming, but this library enables to define luigi tasks and workflows
in the following way:

```python
import luigi
import sciluigi as sl
import math
from subprocess import call
import subprocess as sub
import sys
import requests
import time

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

# Split a file
class ExistingData(sl.SciLuigiExternalTask):
    file_name = luigi.Parameter(default='acgt.txt')

    def output(self):
        return sl.create_file_targets(
             acgt = 'data/' + self.file_name)

class SplitAFile(sl.SciLuigiTask):
    indata = sl.TargetSpecParameter()

    def output(self):
        return sl.create_file_targets(
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
class DoSomething(sl.SciLuigiTask):
    indata = sl.TargetSpecParameter()

    def output(self):
        return sl.create_file_targets(
            outdata = self.get_path('indata') + '.something_done')

    def run(self):
        with self.input('indata').open() as infile, self.output()['outdata'].open('w') as outfile:
            for line in infile:
                outfile.write(line.lower() + '\n')


# Merge the results of the programs
class MergeFiles(sl.SciLuigiTask):
    part1 = sl.TargetSpecParameter()
    part2 = sl.TargetSpecParameter()

    def output(self):
        return sl.create_file_targets(
            merged = self.input('part1').path + '.merged'
        )

    def run(self):
        sub.call('cat {f1} {f2} > {out}'.format(
            f1=self.input('part1').path,
            f2=self.input('part2').path,
            out=self.output()['merged'].path),
        shell=True)

# ------------------------------------------------------------------------
# Workflow class
# ------------------------------------------------------------------------

class MyWorkflow(sl.WorkflowTask):

    task = luigi.Parameter()

    def requires(self):

        tasks = {}

        # Workflow definition goes here!

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

        return tasks[self.task]

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
