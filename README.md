Scientific Luigi
================

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
# Task classes
# ------------------------------------------------------------------------

class ExistingData(sciluigi.SciLuigiExternalTask):
    file_name = luigi.Parameter(default='acgt.txt')

    def output(self):
        return { 'acgt' : luigi.LocalTarget('data/' + self.file_name) }


class SplitAFile(sciluigi.SciLuigiTask):
    indata = sciluigi.TargetSpecParameter()

    def output(self):
        return { 'part1' : self.new_target(dep='indata', ext='.part1'),
                 'part2' : self.new_target(dep='indata', ext='.part2') }

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


class DoSomething(sciluigi.SciLuigiTask):
    indata = sciluigi.TargetSpecParameter()

    def output(self):
        return { 'outdata' : luigi.LocalTarget(self.get_path('indata') + '.something_done' ) }

    def run(self):
        with self.input('indata').open() as infile, self.output()['outdata'].open('w') as outfile:
            for line in infile:
                outfile.write(line.lower() + '\n')


class MergeFiles(sciluigi.SciLuigiTask):
    part1 = luigi.Parameter()
    part2 = luigi.Parameter()

    def output(self):
        return { 'merged' : luigi.LocalTarget(self.input('part1').path + '.merged' ) }

    def run(self):
        sub.call('cat {f1} {f2} > {out}'.format(
            f1=self.input('part1').path,
            f2=self.input('part2').path,
            out=self.output()['merged'].path),
        shell=True)

# ------------------------------------------------------------------------
# Workflow class
# ------------------------------------------------------------------------

class MyWorkflow(luigi.Task):

    task = luigi.Parameter()

    def requires(self):
        '''
		Workflow definition goes here!
		'''
        tasks = {}

		# Split a file
        tasks['split_indata'] = ExistingData()
        tasks['split'] = SplitAFile(
                indata = tasks['split_indata'].outspec('acgt'))

		# Run the same program on both parts of the split
        tasks['dosth1'] = DoSomething(
                indata = tasks['split'].outspec('part1'))
        tasks['dosth2'] = DoSomething(
                indata = tasks['split'].outspec('part2'))

		# Merge the results of the programs
        tasks['merge'] = MergeFiles(
                part1 = tasks['dosth1'].outspec('outdata'),
                part2 = tasks['dosth2'].outspec('outdata'))

		# Return task asked for with --task flag on command line
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
