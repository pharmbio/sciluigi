'''
This module contains functionality related to integration with the SLURM HPC
resource manger.
'''

import datetime
import logging
import re
import time
import sciluigi.parameter
import sciluigi.task
import subprocess as sub

# ================================================================================

# Setup logging
log = logging.getLogger('sciluigi-interface')

# A few 'constants'
RUNMODE_LOCAL = 'runmode_local'
RUNMODE_HPC = 'runmode_hpc'
RUNMODE_MPI = 'runmode_mpi'

# ================================================================================

class SlurmInfo():
    '''
    A data object for keeping slurm run parameters.
    '''
    runmode = None # One of RUNMODE_LOCAL|RUNMODE_HPC|RUNMODE_MPI
    project = None
    partition = None
    cores = None
    time = None
    jobname = None
    threads = None

    def __init__(self, runmode, project, partition, cores, time, jobname, threads):
        '''
        Init a SlurmInfo object, from string data.
        Time is on format: [[[d-]HH:]MM:]SS
        '''
        self.runmode = runmode
        self.project = project
        self.partition = partition
        self.cores = cores
        self.time = time
        self.jobname = jobname
        self.threads = threads

    def __str__(self):
        '''
        Return a readable string representation of the info stored
        '''
        strrepr = ('(time: {t}, '
                   'partition: {pt}, '
                   'cores: {c}, '
                   'threads: {thr}, '
                   'jobname: {j}, '
                   'project: {pr})').format(
                t=self.time,
                pt=self.partition,
                c=self.cores,
                thr=self.threads,
                j=self.jobname,
                pr=self.project)
        return strrepr

    def get_argstr_hpc(self):
        '''
        Return a formatted string with arguments and option flags to SLURM
        commands such as salloc and sbatch, for non-MPI, HPC jobs.
        '''
        argstr = ' -A {pr} -p {pt} -n {c} -t {t} -J {j} srun -n 1 -c {thr} '.format(
                pr=self.project,
                pt=self.partition,
                c=self.cores,
                t=self.time,
                j=self.jobname,
                thr=self.threads)
        return argstr

    def get_argstr_mpi(self):
        '''
        Return a formatted string with arguments and option flags to SLURM
        commands such as salloc and sbatch, for MPI jobs.
        '''
        argstr = ' -A {pr} -p {pt} -n {c1} -t {t} -J {j} mpirun -v -np {c2} '.format(
                pr=self.project,
                pt=self.partition,
                c1=self.cores,
                t=self.time,
                j=self.jobname,
                c2=self.cores)
        return argstr

# ================================================================================

class SlurmInfoParameter(sciluigi.parameter.Parameter):
    '''
    A specialized luigi parameter, taking SlurmInfo objects.
    '''
    def parse(self, x):
        if x is SlurmInfo or x is None:
            return x
        else:
            raise Exception('Parameter is not SlurmInfoParameter: %s' % x)

# ================================================================================

class SlurmHelpers():
    '''
    Mixin with various convenience methods for executing jobs via SLURM
    '''
    # Other class-fields
    slurminfo = SlurmInfoParameter(default=None) # Class: SlurmInfo

    # Main Execution methods
    def ex(self, command):
        '''
        Execute either locally or via SLURM, depending on config
        '''
        if isinstance(command, list):
            command = ' '.join(command)

        if self.slurminfo.runmode == RUNMODE_LOCAL:
            log.info('Executing command in local mode: %s', command)
            self.ex_local(command) # Defined in task.py
        elif self.slurminfo.runmode == RUNMODE_HPC:
            log.info('Executing command in HPC mode: %s', command)
            self.ex_hpc(command)
        elif self.slurminfo.runmode == RUNMODE_MPI:
            log.info('Executing command in MPI mode: %s', command)
            self.ex_mpi(command)


    def ex_hpc(self, command):
        '''
        Execute command in HPC mode
        '''
        if isinstance(command, list):
            command = sub.list2cmdline(command)

        fullcommand = 'salloc %s %s' % (self.slurminfo.get_argstr_hpc(), command)
        (retcode, stdout, stderr) = self.ex_local(fullcommand)

        self.log_slurm_info(stderr)
        return (retcode, stdout, stderr)


    def ex_mpi(self, command):
        '''
        Execute command in HPC mode with MPI support (multi-node, message passing interface).
        '''
        if isinstance(command, list):
            command = sub.list2cmdline(command)

        fullcommand = 'salloc %s %s' % (self.slurminfo.get_argstr_mpi(), command)
        (retcode, stdout, stderr) = self.ex_local(fullcommand)

        self.log_slurm_info(stderr)
        return (retcode, stdout, stderr)


    # Various convenience methods

    def assert_matches_character_class(self, char_class, a_string):
        '''
        Helper method, that tests whether a string matches a regex character class.
        '''
        if not bool(re.match('^{c}+$'.format(c=char_class), a_string)):
            raise Exception('String {s} does not match character class {cc}'.format(
                s=a_string, cc=char_class))

    def clean_filename(self, filename):
        '''
        Clean up a string to make it suitable for use as file name.
        '''
        return re.sub('[^A-Za-z0-9\_\ ]', '_', str(filename)).replace(' ', '_')

    #def get_task_config(self, name):
    #    return luigi.configuration.get_config().get(self.task_family, name)

    def log_slurm_info(self, slurm_stderr):
        '''
        Parse information of the following example form:

        salloc: Granted job allocation 5836263
        srun: Job step created
        salloc: Relinquishing job allocation 5836263
        salloc: Job allocation 5836263 has been revoked.
        '''

        matches = re.search('[0-9]+', slurm_stderr)
        if matches:
            jobid = matches.group(0)

            # Write slurm execution time to audit log
            cmd = '/usr/bin/sacct -j {jobid} --noheader --format=elapsed'.format(jobid=jobid)
            (_, jobinfo_stdout, _) = self.ex_local(cmd)
            sacct_matches = re.findall('([0-9\:\-]+)', jobinfo_stdout)

            if len(sacct_matches) < 2:
                log.warn('Not enough matches from sacct for task %s: %s',
                    self.instance_name, ', '.join(['Match: %s' % m for m in sacct_matches])
                    )
            else:
                slurm_exectime_fmted = sacct_matches[1]
                # Date format needs to be handled differently if the days field is included
                if '-' in slurm_exectime_fmted:
                    tobj = time.strptime(slurm_exectime_fmted, '%d-%H:%M:%S')
                    self.slurm_exectime_sec = int(datetime.timedelta(
                        tobj.tm_mday,
                        tobj.tm_sec,
                        0,
                        0,
                        tobj.tm_min,
                        tobj.tm_hour).total_seconds())
                else:
                    tobj = time.strptime(slurm_exectime_fmted, '%H:%M:%S')
                    self.slurm_exectime_sec = int(datetime.timedelta(
                        0,
                        tobj.tm_sec,
                        0,
                        0,
                        tobj.tm_min,
                        tobj.tm_hour).total_seconds())

                log.info('Slurm execution time for task %s was %ss',
                    self.instance_name,
                    self.slurm_exectime_sec)
                self.add_auditinfo('slurm_exectime_sec', int(self.slurm_exectime_sec))

            # Write this last, so as to get the main task exectime and slurm exectime together in
            # audit log later
            self.add_auditinfo('slurm_jobid', jobid)

# ================================================================================

class SlurmTask(SlurmHelpers, sciluigi.task.Task):
    '''
    luigi task that includes the SlurmHelpers mixin.
    '''
    pass
