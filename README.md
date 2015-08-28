Scientific Luigi
================

**Note: this library is still work in progress, but it is fast nearing completion, and right now being put in production (as of August 29, 2015)**

Scientific Luigi (or SciLuigi, for short) is a light-weight wrapper library around Spotify's [Luigi](http://github.com/spotify/luigi)
workflow system that aims to make writing scientific workflows consisting of
numerous interdependent commandline applicatoins, more fluent, flexible and
modular.

It was designed to solve some very real problem we were facing when trying
to use luigi for defining complex workflows for data preprocessing and
machine-learning, including cross-validation.

Specifically, SciLuigi provides the following features over vanilla Luigi:

- Separates the dependency definitions from the tasks themselves,
  by defining dependencies "from the outside", in a workflow task,
  where the tasks are both instantiated, and stitched together.
- Specify dependencies between individual outputs and inputs of tasks,
  rather than just between the tasks themselves to better capture the
  minutiae of the data dependency network. (This also avoids creating
  accidental dependencies between specific tasks, by task specific
  lookups of dict structures returned by upstream tasks.)
- Make all inputs and outputs to behave like object fields, so as to
  allow auto-completion support to ease the network connection work.
- Set up good default logging configuration for workflow centric tasks
  (Luigi internal logging is turned down to only log warnings and errors,
  while sciluigi by default logs high-level actions such as task starts,
  finishes, and execution times.)
- Produce an easy to read audit-log with high level information per task
  when the workflow task has finished.

The basic idea behind SciLuigi, and a preceding solution to it, was
presented in [this Workshop talk (YouTube)](https://www.youtube.com/watch?v=f26PqSXZdWM)

In terms of code, SciLuigi enables to define luigi tasks and workflows
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
        rawdata = sl.new_task('rawdata', ExistingData, self, file_name='acgt.txt')

        split = sl.new_task('split', SplitAFile, self)
        split.in_data = rawdata.out_acgt

        # Run the same task on the two splits
        dosth1 = sl.new_task('dosth1', DoSomething, self)
        dosth1.in_data = split.out_part1

        dosth2 = sl.new_task('dosth2', DoSomething, self)
        dosth2.in_data = split.out_part2

        # Merge the results
        merge = sl.new_task('merge', MergeFiles, self)
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
    sl.run_local()
```

Then you would run this as:

```bash
python myworkflow.py MyWorkflow --task merge
```
