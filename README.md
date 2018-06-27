![SciLuigi Logo](http://i.imgur.com/2aMT04J.png)

# Scientific Luigi 
(SciLuigi for short) is a light-weight wrapper library around [Spotify](http://spotify.com)'s [Luigi](http://github.com/spotify/luigi) workflow system that aims to make writing scientific workflows more fluent, flexible and modular.

Luigi is a flexile and fun-to-use library. It has turned out though that its default way of defining dependencies by hard coding  them in each task's requires() function is not optimal for some type of workflows common e.g. in bioinformatics where multiple inputs and outputs, complex dependencies, and the need to quickly try different workflow connectivity in an explorative fashion is central to the way of working.

Sciluigi can (optionally) complete tasks by running commands in containers. This can improve reproducibility (as a container can be portably run on the cloud, on private clusters, or for lightweight tasks on a users computer via docker) and ease of use (not requiring the end-user of a workflow to install finicky bioinformatics software while avoiding the problem of conflicting dependencies). Sciluigi can facilitate running software that only runs on linux when hosted on a Windows or Macintosh computer, and leverage cloud computing resources (AWS batch).

SciLuigi was designed to solve some of these problems, by providing the following "features" over vanilla Luigi:

- Separation of dependency definitions from the tasks themselves,
  for improved modularity and composability.
- Inputs and outputs implemented as separate fields, a.k.a.
  "ports", to allow specifying dependencies between specific input
  and output-targets rather than just between tasks. This is again to let such
  details of the network definition reside outside the tasks.
- The fact that inputs and outputs are object fields, also allows auto-completion
  support to ease the network connection work (Works great e.g. with [jedi-vim](https://github.com/davidhalter/jedi-vim)).
- Inputs and outputs are connected with an intuitive "single-assignment syntax".
- "Good default" high-level logging of workflow tasks and execution times.
- Produces an easy to read audit-report with high level information per task.
- Integration with some HPC workload managers, currently AWS batch.
- Integration with cloud-bucket stores (currently AWS S3).
- When containers are used, one can prototype and test a task on test data locally
 with docker, and then run it on cloud resources (e.g. AWS batch) when confronted
 with a large dataset with only a change in a single parameter.

Because of Luigi's easy-to-use API these changes have been implemented
as a very thin layer on top of luigi's own API with no changes at all to the luigi
core, which means that you can continue leveraging the work already being
put into maintaining and further developing luigi by the team at Spotify and others.

* ***UPDATE, Nov, 2016: A paper with the motivation and design decisions behind SciLuigi [now available](http://dx.doi.org/10.1186/s13321-016-0179-6)***
  * If you use SciLuigi in your research, please cite it like this:<br>
    Lampa S, Alvarsson J, Spjuth O. Towards agile large-scale predictive modelling in drug discovery with flow-based programming design principles. *J Cheminform*. 2016. doi:[10.1186/s13321-016-0179-6](http://dx.doi.org/10.1186/s13321-016-0179-6).*
* ***See a poster describing the motivations behind SciLuigi [here](http://dx.doi.org/10.13140/RG.2.1.1143.6246)***

## Support: Getting help

Please use the [issue queue](https://github.com/pharmbio/sciluigi/issues) for any support questions, rather than mailing the author(s) directly, as the solutions can then help others who face similar issues (we are a very small team with very limited time, so this is important).

## Prerequisites

- Python 2.7 - 3.4
- Luigi 1.3.x - 2.0.1
- boto3 > 1.7.10
- docker >= 3.2.1

## Install

1. Install SciLuigi, including its dependencies (luigi etc), through PyPI:

    ```bash
    pip install sciluigi
    ```

2. Now you can use the library by just importing it in your python script, like so:

    ```python
    import sciluigi
    ```

    Note that you can aliase it to a shorter name, for brevity, and to save keystrokes:

    ```python
    import sciluigi as sl
    ```

## Usage

Creating workflows in SciLuigi differs slightly from how it is done in vanilla Luigi.
Very briefly, it is done in these main steps:

1. Create a workflow tasks class
2. Create task classes
3. Add the workflow definition in the workflow class's `workflow()` method.
4. Add a run method at the end of the script
5. Run the script

### Create a Workflow task

The first thing to do when creating a workflow, is to define a workflow task.

You do this by:

1. Creating a subclass of `sciluigi.WorkflowTask`
2. Implementing the `workflow()` method.

#### Example:

```python
import sciluigi

class MyWorkflow(sciluigi.WorkflowTask):
    def workflow(self):
        pass # TODO: Implement workflow here later!
```

### Create tasks

Then, you need to define some tasks that can be done in this workflow.

This is done by:

1. Creating a subclass of `sciluigi.ContainerTask`
2. Adding fields named `in_<yournamehere>` for each input, in the new task class
3. Define methods named `out_<yournamehere>()` for each output, that return `sciluigi.ContainerTargetInfo` objects. sciluigi.TargetInfo is initialized with a reference to the task object itself - typically `self` - and an url. ContainerTargets can silently change where they are hosted, including on local filesystems (/path/to/file.txt) or in buckets (s3://bucket/key/file.txt).
4. Define luigi parameters to the task.
5. Define the container engine and parameters that the container will be run.
6. Implement the `run()` method of the task.

#### Example:

##### Let's define a simple task that just writes "foo" to a file named `foo.txt`.

For this very simple task, we do not need a container, and thus we can base the task on the sciluigi.Task class. We do use the sciluigi.ContainerTargetInfo class here. The path/url we gave is for the local filesystem. If instead we gave an S3 bucket/key url (s3://bucket/foo.txt), this class will handle uploading (and later downloading if needed) from S3.

```python
class MyFooWriter(sciluigi.Task):
    # We have no inputs here
    # Define outputs:
    def out_foo(self):
        return sciluigi.ContainerTargetInfo(self, 'foo.txt')
    def run(self):
        with self.out_foo().open('w') as foofile:
            foofile.write('foo\n')
```

##### Then, let's create a task that replaces "foo" with "bar":

This task will be run in a container, in this case, the alpine linux container. This way (say if we are running sciluigi on a Windows machine without sed), we can still run the command wihtout fuss. In fact, no matter where this is hosted, the task will reliably run in the docker container the same way.

```python
class MyFooReplacer(sciluigi.ContainerTask):
    container = 'alpine:3.7'
    replacement = sciluigi.Parameter() # Here, we take as a parameter
                                  # what to replace foo with.
    # Here we have one input, a "foo file":
    in_foo = None
    # ... and an output, a "bar file":
    def out_replaced(self):
        # As the path to the returned target(info), we
        # use the path of the foo file:
        return sciluigi.ContainerTargetInfo(self, self.in_foo().path + '.bar.txt')
    def run(self):
        self.ex(
            command="sed 's/foo/$repl/g' $infile > $outfile",
            input_targets={
                'infile': self.in_foo(),
            },
            output_targets={
                'outfile': self.out_replaced(),
            },
            extra_parameters={
                'repl': self.replacement,
            }
        )
```
Several things have happened here:

- We've specified which container the command should be run in. This can be any docker-style URI
- The command now uses the [python string template system](https://docs.python.org/3.5/library/string.html#string.Template) to replace parameters, input and output targets
- We use a ContainerTargetInfo in place of a ContainerTarget. This replacement target takes a URL, and can seemlessly handle
local files, S3 buckets (and in the future SFTP, etc).

### Write the workflow definition

Now, we can use these two tasks we created, to create a simple workflow, in our workflow class, that we also created above.

We do this by:

1. Instantiating the tasks, using the `self.new_task(<unique_taskname>, <task_class>, *args, **kwargs)` method, of the workflow task.
2. Connect the tasks together, by pointing the right `out_*` method to the right `in_*` field.
3. Giving some basic parameters as to which sort of container engine should be used for the container task via defining a `sciluigi.ContainerInfo` class.
4. Returning the last task in the chain, from the workflow method.

#### Example:

```python
import sciluigi
class MyWorkflow(sciluigi.WorkflowTask):
    def workflow(self):
        foowriter = self.new_task(
            'foowriter',
            MyFooWriter
        )
        fooreplacer = self.new_task(
            'fooreplacer',
            MyFooReplacer,
            containerinfo=sciluigi.ContainerInfo(
                vcpu=1,
                mem=512,
                engine='docker',
            ),
            replacement='bar'
        )

        # Here we do the *magic*: Connecting outputs to inputs:
        fooreplacer.in_foo = foowriter.out_foo

        # Return the last task(s) in the workflow chain.
        return fooreplacer
```

### Add a run method to the end of the script

Now, the only thing that remains, is adding a run method to the end of the script.

You can use luigi's own `luigi.run()`, or our own two methods:

1. `sciluigi.run()`
2. `sciluigi.run_local()`

The `run_local()` one, is handy if you don't want to run a central scheduler daemon, but just want to run the workflow as a script.

Both of the above take the same options as `luigi.run()`, so you can for example set the main class to use (our workflow task):

```
# End of script ....
if __name__ == '__main__':
    sciluigi.run_local(main_task_cls=MyWorkflow)
```

### Run the workflow

Now, you should be able to run the workflow as simple as:

```bash
python myworkflow.py
```

... provided of course, that the workflow is saved in a file named myworkflow.py.

### More Examples

See the [examples folder](https://github.com/samuell/sciluigi/tree/master/examples) for more detailed examples!

### More links, background info etc.

The basic idea behind SciLuigi, and a preceding solution to it, was
presented in workshop (e-Infra MPS 2015) talk:
- [Slides](http://www.slideshare.net/SamuelLampa/building-workflows-with-spotifys-luigi)
- [Video](https://www.youtube.com/watch?v=f26PqSXZdWM)

See also [this collection of links](http://bionics.it/posts/our-experiences-using-spotifys-luigi-for-bioinformatics-workflows), to more of our reported experiences
using Luigi, which lead up to the creation of SciLuigi.

Known Limitations
-----------------

- Changing the workflow scheduling based on data sent as parameters, is not
  possible.
- If you have an unknown number of outputs from a task, for which you want to
  start a full branch of the workflow, this is not possible either.

Both of the limitations are due to the fact that Luigi does scheduling and
execution separately (with the exception of Luigi's [dynamic dependencies](http://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies),
but they work only for upstream tasks, not downstream tasks, which we would
need).

If you run into any of these problems, you might be interested in a new workflow engine we develop to overcome these limitations:
[SciPipe](http://scipipe.org).

Changelog
---------
- 0.9.6b7_ct
  - Support for containerized tasks and `ContainerTargetInfo`
- 0.9.3b4
  - Support for Python 3 (Thanks to @jeffcjohnson for contributing this!).
  - Bug fixes.

Contributors
----------------
- [See here](https://github.com/pharmbio/sciluigi/graphs/contributors)

Acknowledgements
----------------
This work is funded by:
- [Faculty grants of the dept. of Pharmaceutical Biosciences, Uppsala University](http://www.farmbio.uu.se)
- [Bioinformatics Infrastructure for Life Sciences, BILS](https://bils.se)

Many ideas and inspiration for the API is taken from:
- [John Paul Morrison's invention and works on Flow-Based Programming](http://jpaulmorrison.com/fbp)

See also: [SciPipe](http://scipipe.org)
---------------------------------------

If you find yourself needing some more advanced scheduling features like dynamic scheduling, or run into performance problems with Python/Luigi/SciLuigi, you might be interested to check out a new workflow engine we develop, in the Go programming language, to cope with some of the limitations we have still faced with Python/Luigi/SciLuigi:
[SciPipe](http://scipipe.org). 

[SciPipe](http://scipipe.org) leverages some of the successful parts of Luigi's API, such as the flexible file name formatting, but replaces the Luigi scheduler with a custom, novel and very light-weight implicit dataflow scheduler written in Go. We find that it makes life much easier for complex workflow constructs as those involving cross validation, and/or nested parameter sweeps.
