Luigi++
=======

Extra helper methods for writing [Luigi](http://github.com/spotify/luigi) workflows.

More info is coming, but this library enables to define luigi tasks and workflows
in the following way:

```python
import luigi
import luigipp
from subprocess import call

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

class RawData(luigipp.LuigiPPExternalTask):
    def output(self):
        return { 'rawdata' : luigi.LocalTarget('rawdata') }

class AToT(luigipp.LuigiPPTask):
    indata = luigi.Parameter()

    def output(self):
        return { 'atotreplaced' : luigi.LocalTarget(self.get_input('indata').path + '.atot') }

    def run(self):
        cmd ='cat ' + self.get_input('indata').path + ' | sed "s/A/T/g" > ' + self.output()['atotreplaced'].path
        print("COMMAND: " + cmd)
        call(cmd, shell=True)

# ------------------------------------------------------------------------
# Workflow class(es)
# ------------------------------------------------------------------------

class MyWorkflow(luigi.Task):

    def requires(self):

        # Workflow definition
        rawdata = RawData()
        atot = AToT(
                indata=rawdata.outport('rawdata')
               )

        return atot

# ------------------------------------------------------------------------
# Run this file as script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    luigi.run()
```

Then you would run this as:

```bash
python example.py --local-scheduler MyWorkflow
```
