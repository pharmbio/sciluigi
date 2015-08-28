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
  greatly improving modularity and composability of tasks.
- Make individual inputs and outputs behave as separate fields, a.k.a.
  "ports", to allow specifying dependencies between specific inputs
  and outputs rather than just between tasks. This is again to let such
  network definition code reside outside the tasks themselves.
- Make all inputs and outputs to behave like object fields, so as to
  allow auto-completion support to ease the network connection work.
- Connect inputs and outputs with an intuitive "single assignment-syntax"
  (Similar to how you assign one value to another, in any programming)
- Set up good default logging configuration for workflow centric tasks
  (Luigi internal logging is turned down to only log warnings and errors,
  while sciluigi by default is set to log high-level actions such as
  task starts, finishes, and execution times.)
- Produce an easy to read audit-log with high level information per task
  when the workflow task has finished.
- Provide some integration with HPC workload managers. So far only [SLURM](http://slurm.schedmd.com/)
  is supported though.

The basic idea behind SciLuigi, and a preceding solution to it, was
presented in [this Workshop talk (YouTube)](https://www.youtube.com/watch?v=f26PqSXZdWM)

In terms of code, SciLuigi enables to define luigi tasks and workflows
in the following way:

```python
import logging
import luigi
import sciluigi as sl
import math

# ------------------------------------------------------------------------
# Set up access to the sciluigi logging
# ------------------------------------------------------------------------

log = logging.getLogger('sciluigi-interface')

# ------------------------------------------------------------------------
# The Main Workflow class
# ------------------------------------------------------------------------

class TestWorkflow(sl.WorkflowTask):
    # ------------------------------------------------
    # Parameters to the workflow
    # ------------------------------------------------
    task = luigi.Parameter() # Task to return, chosable on commandline

    # ------------------------------------------------
    # The workflow definition goes here below!
    # ------------------------------------------------
    def workflow(self):
        # ------------------------------------------------
        # Initialize tasks, providing a task name, and a class
        # ------------------------------------------------
        rawdata = self.new_task('rawdata', ExistingData,
                file_name='acgt.txt')
        split = self.new_task('run10min', SplitAFile)
        dosth1 = self.new_task('dosth1', DoSomething)
        dosth2 = self.new_task('dosth2', DoSomething)
        merge = self.new_task('merge', MergeFiles)

        # ------------------------------------------------
        # This is how you connect the data dependency graph!!
        # ------------------------------------------------
        split.in_data = rawdata.out_acgt
        dosth1.in_data = split.out_part1
        dosth2.in_data = split.out_part2
        merge.in_part1 = dosth1.out_data
        merge.in_part2 = dosth2.out_data

        # ------------------------------------------------
        # Return a task by its variable name
        # ------------------------------------------------
        return locals()[self.task]

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

class ExistingData(sl.ExternalTask):
    '''
    A simple task just returning a file target upon request
    '''

    # Params
    file_name = luigi.Parameter(default='acgt.txt')

    # Out ports
    def out_acgt(self):
        return sl.TargetInfo(self, 'data/' + self.file_name)


class SplitAFile(sl.Task):
    '''
    Split a file in two roughly equal size (in terms of rows)
    '''

    # In ports
    in_data = None

    # Out ports
    def out_part1(self):
        return sl.TargetInfo(self, self.in_data().path + '.part1')
    def out_part2(self):
        return sl.TargetInfo(self, self.in_data().path + '.part2')

    # Implementation
    def run(self):
        cmd = 'wc -l {f}'.format(f=self.in_data().path ) # <- How inputs are accessed!
        status, wc_output, stderr = self.ex(cmd)

        lines_cnt = int(wc_output.split(' ')[0])
        head_cnt = int(math.ceil(lines_cnt / 2))
        tail_cnt = int(math.floor(lines_cnt / 2))

        cmd_head = 'head -n {cnt} {i} > {part1}'.format(
            i=self.in_data().path,
            cnt=head_cnt,
            part1=self.out_part1().path) # <- ...and this is how outputs are accessed!
        log.info("COMMAND: " + cmd_head)
        self.ex(cmd_head)

        self.ex('tail -n {cnt} {i} > {part2}'.format(
            cnt=tail_cnt,
            i=self.in_data().path,
            part2=self.out_part2().path))


class DoSomething(sl.Task):
    '''
    Run the same program on both parts of the split
    '''

    # In-ports
    in_data = None

    # Out-ports
    def out_data(self):
        return sl.TargetInfo(self, self.in_data().path + '.something_done')

    # Implementation
    def run(self):
        with self.in_data().open() as infile, self.out_data().open('w') as outfile:
            for line in infile:
                outfile.write(line.lower() + '\n')


class MergeFiles(sl.Task):
    '''
    Merge the results of the programs
    '''

    # In-ports
    in_part1 = None
    in_part2 = None

    # Out-ports
    def out_merged(self):
        return sl.TargetInfo(self, self.in_part1().path + '.merged')

    # Implementation
    def run(self):
        self.ex('cat {f1} {f2} > {out}'.format(
            f1=self.in_part1().path,
            f2=self.in_part2().path,
            out=self.out_merged().path))

# ------------------------------------------------------------------------
# Run as script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    sl.run_local(main_task_cls=TestWorkflow, cmdline_args=['--task=merge'])
```

Then you would run this as:

```bash
python myworkflow.py
```
