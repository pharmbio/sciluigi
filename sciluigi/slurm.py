import datetime
import logging
import luigi
import parameter
import re
import time

# ================================================================================

# Setup logging
log = logging.getLogger('sciluigi-interface')

# A few 'constants'
RUNMODE_LOCAL = 'runmode_local'
RUNMODE_HPC = 'runmode_hpc'
RUNMODE_MPI = 'runmode_mpi'

# ================================================================================

class SlurmInfo():
    runmode = None # One of RUNMODE_LOCAL|RUNMODE_HPC|RUNMODE_MPI
    project = None
    partition = None
    cores = None
    time = None
    jobname = None
    threads = None

    def __init__(self, runmode, project, partition, cores, time, jobname, threads):
        self.runmode = runmode
        self.project = project
        self.partition = partition
        self.cores = cores
        self.time = time
        self.jobname = jobname
        self.threads = threads

    def get_argstr_hpc(self):
        '''
        Return a formatted string with arguments and option flags to SLURM
        commands such as salloc and sbatch, for non-MPI, HPC jobs.
        '''
        argstr = ' -A {pr} -p {pt} -n {c} -t {t} -J {j} srun -n 1 -c {thr} '.format(
                pr = self.project,
                pt = self.partition,
                c = self.cores,
                t = self.time,
                j = self.jobname,
                thr = self.threads)
        return argstr

    def get_argstr_mpi(self):
        '''
        Return a formatted string with arguments and option flags to SLURM
        commands such as salloc and sbatch, for MPI jobs.
        '''
        argstr = ' -A {pr} -p {pt} -n {c} -t {t} -J {j} mpirun -v -np {c} '.format(
                pr = self.project,
                pt = self.partition,
                c = self.cores,
                t = self.time,
                j = self.jobname)
        return argstr

# ================================================================================

class SlurmInfoParameter(parameter.Parameter):
    def parse(self, x):
        # TODO: Possibly do something more fancy here
        return x

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
        command_str = ' '.join(command)
        if self.slurminfo.runmode == RUNMODE_LOCAL:
            log.info('Executing command in local mode: %s' % command_str)
            self.ex_local(command) # Defined in task.py
        elif self.slurminfo.runmode == RUNMODE_HPC:
            log.info('Executing command in HPC mode: %s' % command_str)
            self.ex_hpc(command)
        elif self.slurminfo.runmode == RUNMODE_MPI:
            log.info('Executing command in MPI mode: %s' % command_str)
            self.ex_mpi(command)


    def ex_hpc(self, command):
        if isinstance(command, list):
            command = ' '.join(command)

        fullcommand = 'salloc %s %s' % (self.slurminfo.get_argstr_hpc(), command)
        (status, output) = self.ex_local(fullcommand)

        # TODO: Do this only if audit logging is activated!
        self.log_slurm_info(output)
        return (status, output)


    def ex_mpi(self, command):
        if isinstance(command, list):
            command = ' '.join(command)

        fullcommand = 'salloc %s %s' % (self.slurminfo.get_argstr_mpi(), command)
        (status, output) = self.ex_local(fullcommand)

        # TODO: Do this only if audit logging is activated!
        self.log_slurm_info(output)
        return (status, output)


    # Various convenience methods

    def assert_matches_character_class(self, char_class, a_string):
        if not bool(re.match('^{c}+$'.format(c=char_class), a_string)):
            raise Exception('String {s} does not match character class {cc}'.format(s=a_string, cc=char_class))

    def clean_filename(self, filename):
        return re.sub('[^A-Za-z0-9\_\ ]', '_', str(filename)).replace(' ', '_')

    #def get_task_config(self, name):
    #    return luigi.configuration.get_config().get(self.task_family, name)

    #FIXME: Fix this big mess below!
    def log_slurm_info(self, command_output):
        matches = re.search('[0-9]+', command_output)
        if matches:
            jobid = matches.group(0)

            # Write slurm execution time to audit log
            (jobinfo_status, jobinfo_output) = self.ex_local('/usr/bin/sacct -j {jobid} --noheader --format=elapsed'.format(jobid=jobid))
            last_line = jobinfo_output.split('\n')[-1]
            sacct_matches = re.search('([0-9\:\-]+)',last_line)

            if sacct_matches:
                slurm_exectime_fmted = sacct_matches.group(1)
                # Date format needs to be handled differently if the days field is included
                if '-' in slurm_exectime_fmted:
                    t = time.strptime(slurm_exectime_fmted, '%d-%H:%M:%S')
                    self.slurm_exectime_sec = int(datetime.timedelta(t.tm_mday, t.tm_sec, 0, 0, t.tm_min, t.tm_hour).total_seconds())
                else:
                    t = time.strptime(slurm_exectime_fmted, '%H:%M:%S')
                    self.slurm_exectime_sec = int(datetime.timedelta(0, t.tm_sec, 0, 0, t.tm_min, t.tm_hour).total_seconds())

                log.info('Slurm execution time for task %s was %ss' % (self.instance_name, self.slurm_exectime_sec))
                self.workflow_task.add_auditinfo(self.instance_name, 'slurm_exectime_sec', int(self.slurm_exectime_sec))
            else:
                log.error('No matches from sacct for task %s' % self.instance_name)

            # Write this last, so as to get the main task exectime and slurm exectime together in audit log later
            self.workflow_task.add_auditinfo(self.instance_name, 'slurm_jobid', jobid)
