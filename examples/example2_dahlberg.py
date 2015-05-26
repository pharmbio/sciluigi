import luigi
import luigipp
import math
from subprocess import call
import subprocess as sub
import sys
import requests
import time

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

#Rsync a folder
class RSyncAFolder(luigipp.LuigiPPTask):
    src_dir_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter()

    def output(self):
        # TODO: Let's see if a folder can be used as a target ...
        return { 'dest_dir' : luigi.LocalTarget(self.dest_dir_path) }

    def run(self):
        call('rsync -a {src}/ {dest}/'.format(
            src = self.src_dir_path,
            dest = self.dest_dir_path),
        shell=True)


#Run a program that takes 10 minutes (seconds now, for a try) to run
class Run10MinuteSleep(luigipp.LuigiPPTask):
    upstream_target = luigi.Parameter()

    def output(self):
        return { 'done_flagfile' : luigi.LocalTarget(self.get_path('upstream_target') + '.10mintask_done' ) }

    def run(self):
        time.sleep(10)
        with self.output()['done_flagfile'].open('w') as flagfile:
            flagfile.write('Done!')


#Perform a web request
class DoWebRequest(luigipp.LuigiPPTask):
    upstream_target = luigi.Parameter()

    def output(self):
        return { 'done_flagfile' : luigi.LocalTarget(self.get_path('upstream_target') + '.webrequest_done' ) }

    def run(self):
        resp = requests.get('http://bils.se')
        if resp.status_code != 200:
            raise Exception('Web request failed!')
            sys.exit()
        else:
            with self.output()['done_flagfile'].open('w') as flagfile:
                flagfile.write('Web Request Task Done!')


#Split a file
class ExistingData(luigipp.LuigiPPExternalTask):
    file_name = luigi.Parameter(default='acgt.txt')

    def output(self):
        return { 'acgt' : luigi.LocalTarget('data/' + self.file_name) }

class SplitAFile(luigipp.LuigiPPTask):
    indata_target = luigi.Parameter()

    def output(self):
        return { 'part1' : luigi.LocalTarget(self.get_path('indata_target') + '.part1'),
                 'part2' : luigi.LocalTarget(self.get_path('indata_target') + '.part2') }

    def run(self):
        cmd = 'wc -l {f}'.format(f=self.get_path('indata_target') )
        wc_output = sub.check_output(cmd, shell=True)
        lines_cnt = int(wc_output.split(' ')[0])
        head_cnt = int(math.ceil(lines_cnt / 2))
        tail_cnt = int(math.floor(lines_cnt / 2))

        cmd_head = 'head -n {cnt} {i} > {part1}'.format(
            i=self.get_path('indata_target'),
            cnt=head_cnt,
            part1=self.output()['part1'].path)
        print("COMMAND: " + cmd_head)
        sub.call(cmd_head, shell=True)

        sub.call('tail -n {cnt} {i} {cnt} > {part2}'.format(
            i=self.get_path('indata_target'),
            cnt=tail_cnt,
            part2=self.output()['part2'].path),
        shell=True)


#Run the same program on both parts of the split
class DoSomething(luigipp.LuigiPPTask):
    indata_target = luigi.Parameter()

    def output(self):
        return { 'outdata' : luigi.LocalTarget(self.get_path('indata_target') + '.something_done' ) }

    def run(self):
        with self.get_input('indata_target').open() as infile, self.output()['outdata'].open('w') as outfile:
            for line in infile:
                outfile.write(line.lower() + '\n')


#Merge the results of the programs
class MergeFiles(luigipp.LuigiPPTask):
    part1_target = luigi.Parameter()
    part2_target = luigi.Parameter()

    def output(self):
        return { 'merged' : luigi.LocalTarget(self.get_input('part1_target').path + '.merged' ) }

    def run(self):
        sub.call('cat {f1} {f2} > {out}'.format(
            f1=self.get_input('part1_target').path,
            f2=self.get_input('part2_target').path,
            out=self.output()['merged'].path),
        shell=True)

# ------------------------------------------------------------------------
# Workflow class
# ------------------------------------------------------------------------

class DahlbergTest(luigi.Task):

    task = luigi.Parameter()

    def requires(self):

        tasks = {}

        # Workflow definition goes here!

        #Rsync en mapp
        tasks['rsync'] = RSyncAFolder(
                src_dir_path = 'data',
                dest_dir_path = 'data_rsynced_copy'
                )

        #Kor ett program som tar 10 minuter att kora
        tasks['run10min'] = Run10MinuteSleep(
                upstream_target = tasks['rsync'].outport('dest_dir')
                )

        #Gora en http request ut
        tasks['webreq'] = DoWebRequest(
                upstream_target = tasks['run10min'].outport('done_flagfile')
                )

        tasks['split_indata'] = ExistingData()

        #Splitta en fil
        tasks['split'] = SplitAFile(
                indata_target = tasks['split_indata'].outport('acgt')
                )

        #Kor samma program pa de tva resultaten
        tasks['dosth1'] = DoSomething(
                indata_target = tasks['split'].outport('part1')
                )

        tasks['dosth2'] = DoSomething(
                indata_target = tasks['split'].outport('part2')
                )

        #Merga resultaten
        tasks['merge'] = MergeFiles(
                part1_target = tasks['dosth1'].outport('outdata'),
                part2_target = tasks['dosth2'].outport('outdata')
                )

        return tasks[self.task]


# ------------------------------------------------------------------------
# Run this file as a script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    luigi.run()
