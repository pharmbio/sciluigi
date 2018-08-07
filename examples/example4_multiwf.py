'''
An example showing how you can run multiple workflow tasks, from a "Meta workflow" (MetaWF below)
'''

import sciluigi as sl
import luigi

class MetaWF(sl.WorkflowTask):
    '''
    Meta workflow
    '''
    def workflow(self):
        tasks = []
        for r in ['bar', 'tjo', 'hej']:
            wf = self.new_task('wf', WF, replacement=r)
            tasks.append(wf)
        return tasks

class WF(sl.WorkflowTask):
    '''
    Main workflow, which is run in multiple instances above
    '''
    replacement = luigi.Parameter()
    def workflow(self):
        t1 = self.new_task('foowriter', FooWriter)
        t2 = self.new_task('foo2bar', Foo2Bar, replacement=self.replacement)
        t2.in_foo = t1.out_foo
        return t2

class FooWriter(sl.Task):
    '''
    A dummy task
    '''
    def out_foo(self):
        return sl.TargetInfo(self, 'foo.txt')
    def run(self):
        self.ex('echo foo > {foo}'.format(
            foo=self.out_foo().path))

class Foo2Bar(sl.Task):
    '''
    Another dummy task
    '''
    replacement = luigi.Parameter()
    in_foo = sl.TargetInfo(None, 'None')
    def out_bar(self):
        return sl.TargetInfo(self, self.in_foo().path + '.{r}.txt'.format(r=self.replacement))
    def run(self):
        self.ex('sed "s/foo/{r}/g" {inf} > {outf}'.format(
            r=self.replacement,
            inf=self.in_foo().path,
            outf=self.out_bar().path)
        )

# Run as script
if __name__ == '__main__':
    sl.run_local()
