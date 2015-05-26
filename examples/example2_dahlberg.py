import luigi
import luigipp
from subprocess import call
import time
import requests
import sys

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

#Rsync a folder
class RSyncAFolder(luigipp.LuigiPPTask):
    src_dir_target = luigi.Parameter()
    dest_dir_path = luigi.Parameter()

    def output(self):
        # TODO: Let's see if a folder can be used as a target ...
        return { 'dest_dir' : luigi.LocalTarget(self.dest_dir) }

    def run(self):
        call('rsync -a {src} {dest}'.format(
            src = self.get_input('src_dir').path,
            dest = self.dest_dir))


#Run a program that takes 10 minutes (seconds now, for a try) to run
class Run10MinuteSleep(luigi.LuigiPPTask):
    upstream_target = luigi.Parameter()

    def output(self):
        return { 'done_flagfile' : luigi.LocalTarget(self.get_input('upstream_target').path + '.10mintask_done' ) }

    def run(self):
        time.sleep(10)


#Perform a web request
class DoWebRequest(luigipp.LuigiPPTask):
    upstream_target = luigi.Parameter()

    def output(self):
        return { 'done_flagfile' : luigi.LocalTarget(self.get_input('upstream_target').path + '.webrequest_done' ) }

    def run(self):
        resp = requests.get('http://bils.se')
        if resp.statu_code != 200:
            raise Exception('Web request failed!')
            sys.exit()
        else:
            with self.output()['done_flagfile'].open('w') as flagfile:
                flagfile.write('Web Request Task Done!')


#Split a file
class SplitAFile(luigipp.LuigiPPTask):
    pass


#Run the same program on both parts of the split
class DoSomething(luigipp.LuigiPPTask):
    pass


#Merge the results of the programs
class MergeFiles(luigipp.LuigiPPTask):
    pass
