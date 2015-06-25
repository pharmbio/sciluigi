Scientific Luigi
================

Extra helper methods for writing scientific workflows in [Luigi](http://github.com/spotify/luigi).

More info is coming, but this library enables to define luigi tasks and workflows
in the following way:

```python
import luigi
import sciluigi as sl
import math
import subprocess as sub

# ------------------------------------------------------------------------
# Workflow
# ------------------------------------------------------------------------

class MyWorkflow(sl.WorkflowTask):
    task = luigi.Parameter() # Task to return, chosable on commandline

    def workflow(self):
        # Split a file
        rawdata = sl.new_task(ExistingData, file_name='acgt.txt')

        split = sl.new_task(SplitAFile)
        split.in_data = rawdata.out_acgt

        # Run the same task on the two splits
        dosth1 = sl.new_task(DoSomething)
        dosth1.in_data = split.out_part1

        dosth2 = sl.new_task(DoSomething)
        dosth2.in_data = split.out_part2

        # Merge the results
        merge = sl.new_task(MergeFiles)
        merge.in_part1 = dosth1.out_data
        merge.in_part2 = dosth2.out_data

        return locals()[self.task]

# ------------------------------------------------------------------------
# Task components
# ------------------------------------------------------------------------

class ExistingData(sl.ExternalTask):
    # Parameters
    file_name = luigi.Parameter(default='acgt.txt')

    # Inputs/Outputs
    def out_acgt(self):
        return sl.TargetInfo(self, 'data/' + self.file_name)


class SplitAFile(sl.Task):
    # Inputs/Outputs
    in_data = None

    def out_part1(self):
        return sl.TargetInfo(self, self.in_data().path + '.part1')
    def out_part2(self):
        return sl.TargetInfo(self, self.in_data().path + '.part2')

    # What the task does
    def run(self):
        cmd = 'wc -l {f}'.format(f=self.in_data().path )
        wc_output = sub.check_output(cmd, shell=True)
        lines_cnt = int(wc_output.split(' ')[0])
        head_cnt = int(math.ceil(lines_cnt / 2))
        tail_cnt = int(math.floor(lines_cnt / 2))

        cmd_head = 'head -n {cnt} {i} > {part1}'.format(
            i=self.in_data().path,
            cnt=head_cnt,
            part1=self.out_part1().path)
        print("COMMAND: " + cmd_head)
        sub.call(cmd_head, shell=True)

        sub.call('tail -n {cnt} {i} {cnt} > {part2}'.format(
            i=self.in_data().path,
            cnt=tail_cnt,
            part2=self.out_part2().path),
        shell=True)


class DoSomething(sl.Task):
    # Inputs/Outputs
    in_data = None

    def out_data(self):
        return sl.TargetInfo(self, self.in_data().path + '.something_done')

    # What the task does
    def run(self):
        with self.in_data().open() as infile, self.out_data().open('w') as outfile:
            for line in infile:
                outfile.write(line.lower() + '\n')


class MergeFiles(sl.Task):
    # Inputs/Outputs
    in_part1 = None
    in_part2 = None

    def out_merged(self):
        return sl.TargetInfo(self, self.in_part1().path + '.merged')

    # What the task does
    def run(self):
        sub.call('cat {f1} {f2} > {out}'.format(
            f1=self.in_part1().path,
            f2=self.in_part2().path,
            out=self.out_merged().path),
        shell=True)

# ------------------------------------------------------------------------
# Run as script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    sl.run_locally()
```

Then you would run this as:

```bash
python myworkflow.py MyWorkflow --task merge
```
