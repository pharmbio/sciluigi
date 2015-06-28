import luigi
import sciluigi as sl
import math
from subprocess import call
import subprocess as sub
import sys
import requests
import time

# ------------------------------------------------------------------------
# Workflow class
# ------------------------------------------------------------------------

class NGITestWF(sl.WorkflowTask):
    task = luigi.Parameter() # Task to return, chosable on commandline

    def workflow(self):
        # Rsync a folder
        rsync = sl.new_task(RSyncAFolder, 'rsync', self,
                src_dir_path = 'data',
                dest_dir_path = 'data_rsynced_copy')

        # Run a program that takes 10 minutes (seconds)
        run10min = sl.new_task(Run10MinuteSleep, 'run10min', self)
        run10min.in_upstream = rsync.out_destdir

        # Do a web request
        webreq = sl.new_task(DoWebRequest, 'run10min', self)
        webreq.in_upstream = run10min.out_doneflag

        # Split a file
        rawdata = sl.new_task(ExistingData, 'rawdata', self,
                file_name='acgt.txt')

        split = sl.new_task(SplitAFile, 'run10min', self)
        split.in_data = rawdata.out_acgt

        # Run the same task on the two splits
        dosth1 = sl.new_task(DoSomething, 'dosth1', self)
        dosth1.in_data = split.out_part1

        dosth2 = sl.new_task(DoSomething, 'dosth2', self)
        dosth2.in_data = split.out_part2

        # Merge the results
        merge = sl.new_task(MergeFiles, 'merge', self)
        merge.in_part1 = dosth1.out_data
        merge.in_part2 = dosth2.out_data

        return locals()[self.task]

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

# Rsync a folder
class RSyncAFolder(sl.Task):

    # Params
    src_dir_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter()

    # I/O
    def out_destdir(self):
        return sl.TargetInfo(self, self.dest_dir_path)

    # Impl
    def run(self):
        call('rsync -a {src}/ {dest}/'.format(
            src = self.src_dir_path,
            dest = self.dest_dir_path),
        shell=True)

# Run a program that takes 10 minutes (seconds now, for a try) to run
class Run10MinuteSleep(sl.Task):

    # I/O
    in_upstream = None

    def out_doneflag(self):
        return sl.TargetInfo(self, self.in_upstream().path + '.10mintask_done')

    # Impl
    def run(self):
        time.sleep(10)
        with self.out_doneflag().open('w') as flagfile:
            flagfile.write('Done!')


# Perform a web request
class DoWebRequest(sl.Task):

    # I/O
    in_upstream = None

    def out_doneflag(self):
        return sl.TargetInfo(self, self.in_upstream().path + '.webrequest_done')

    # Impl
    def run(self):
        resp = requests.get('http://bils.se')
        if resp.status_code != 200:
            raise Exception('Web request failed!')
            sys.exit()
        else:
            with self.out_doneflag().open('w') as flagfile:
                flagfile.write('Web Request Task Done!')


class ExistingData(sl.ExternalTask):

    # Params
    file_name = luigi.Parameter(default='acgt.txt')

    # I/O
    def out_acgt(self):
        return sl.TargetInfo(self, 'data/' + self.file_name)


class SplitAFile(sl.Task):

    # I/O
    in_data = None

    def out_part1(self):
        return sl.TargetInfo(self, self.in_data().path + '.part1')

    def out_part2(self):
        return sl.TargetInfo(self, self.in_data().path + '.part2')

    # Impl
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
    '''
    Run the same program on both parts of the split
    '''

    # I/O
    in_data = None

    def out_data(self):
        return sl.TargetInfo(self, self.in_data().path + '.something_done')

    # Impl
    def run(self):
        with self.in_data().open() as infile, self.out_data().open('w') as outfile:
            for line in infile:
                outfile.write(line.lower() + '\n')


class MergeFiles(sl.Task):
    '''
    Merge the results of the programs
    '''

    # I/O
    in_part1 = None
    in_part2 = None

    def out_merged(self):
        return sl.TargetInfo(self, self.in_part1().path + '.merged')

    # Impl
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
