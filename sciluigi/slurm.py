import luigi
import time

# ================================================================================

class SlurmInfo(object):
    def __init__(self, project, partition, cores, time, jobname, threads):
        self.project = project
        self.partition = partition
        self.cores = cores
        self.time = time
        self.jobname = jobname
        self.threads = threads

    def get_argstr(self):
        '''
        Return a formatted string with arguments and option flags to SLURM
        commands such as salloc and sbatch.
        '''
        argstr = ' -A {pr} -p {pt} -n {c} -t {t} -J {j} srun -n 1 -c {thr} '.format(
                pr = self.project,
                pt = self.partition,
                c = self.cores,
                t = self.time,
                j = self.jobname,
                thr = self.threads)
        return argstr


class SlurmHelpers():
    '''
    Mixin with various convenience methods for executing jobs via SLURM
    '''
    accounted_project = luigi.Parameter()

    # A few 'constants'
    JOBTYPE_LOCAL = 'jobtype_local'
    JOBTYPE_HPC = 'jobtype_hpc'
    JOBTYPE_MPI = 'jobtype_mpi'

    # Main Execution methods
    def execute_in_configured_mode(self, command):
       '''Execute either locally or via SLURM, depending on config'''

        if self.get_task_config('runmode') == self.JOBTYPE_LOCAL:
            self.execute_command(command)

        elif self.get_task_config('runmode') == self.JOBTYPE_HPC:
            train_size = 'NA'
            if hasattr(self, 'train_size'):
                train_size = self.train_size
            replicate_id = 'NA'
            if hasattr(self, 'replicate_id'):
                replicate_id = self.replicate_id

            self.execute_hpcjob(command,
                    accounted_project = self.accounted_project,
                    time_limit = self.get_task_config('time_limit'),
                    partition  = self.get_task_config('partition'),
                    cores      = self.get_task_config('cores'),
                    jobname    = ''.join([train_size,
                                          replicate_id,
                                          self.dataset_name,
                                          self.task_family]),
                    threads    = self.get_task_config('threads'))

        elif self.get_task_config('runmode') == self.JOBTYPE_MPI:
            self.execute_mpijob(command,
                    accounted_project = self.accounted_project,
                    time_limit = self.get_task_config('time_limit'),
                    partition  = self.get_task_config('partition'),
                    cores      = self.get_task_config('cores'),
                    jobname    = ''.join([self.train_size,
                                          self.replicate_id,
                                          self.dataset_name,
                                          self.task_family]))

    def execute_command(self, command):

        if isinstance(command, list):
            command = ' '.join(command)

        log.info('Executing command: ' + str(command))
        (status, output) = commands.getstatusoutput(command)
        log.info('STATUS: ' + str(status))
        log.info('OUTPUT: ' + '; '.join(str(output).split('\n')))
        if status != 0:
            log.error('Command failed: {cmd}'.format(cmd=command))
            log.error('OUTPUT OF FAILED COMMAND: ' + '; \n'.join(str(output).split('\n')))
            raise Exception('Command failed: {cmd}\nOutput:\n{output}'.format(cmd=command, output=output))
        return (status, output)

    def execute_hpcjob(self, command, accounted_project, time_limit='4:00:00', partition='node', cores=16, jobname='LuigiHPCJob', threads=16):

        slurm_part = 'salloc -A {pr} -p {pt} -n {c} -t {t} -J {m} srun -n 1 -c {thr} '.format(
                pr  = accounted_project,
                pt  = partition,
                c   = cores,
                t   = time_limit,
                m   = jobname,
                thr = threads)

        if isinstance(command, list):
            command = ' '.join(command)

        (status, output) = self.execute_command(slurm_part + command)

        # TODO: Do this only if audit logging is activated!
        self.log_slurm_info(output)

        return (status, output)

    def execute_mpijob(self, command, accounted_project, time_limit='4-00:00:00', partition='node', cores=32, jobname='LuigiMPIJob', cores_per_node=16):

        slurm_part = 'salloc -A {pr} -p {pt} -n {c} -t {t} -J {m} mpirun -v -np {c} '.format(
                pr = accounted_project,
                pt = partition,
                c  = cores,
                t  = time_limit,
                m  = jobname)

        if isinstance(command, list):
            command = ' '.join(command)

        (status, output) = self.execute_command(slurm_part + command)

        # TODO: Do this only if audit logging is activated!
        self.log_slurm_info(output)

        return (status, output)

    def execute_locally(self, command):
        '''Execute locally only'''
        return self.execute_command(command)

    def x(self, command):
        '''A short-hand alias around the execute_in_configured_mode method'''
        return self.execute_in_configured_mode(command)

    def lx(self, command):
        '''Short-hand alias around the execute_locally method'''
        return self.execute_locally(command)


    # Various convenience methods

    def assert_matches_character_class(self, char_class, a_string):
        if not bool(re.match('^{c}+$'.format(c=char_class), a_string)):
            raise Exception('String {s} does not match character class {cc}'.format(s=a_string, cc=char_class))

    def clean_filename(self, filename):
        return re.sub('[^A-Za-z0-9\_\ ]', '_', str(filename)).replace(' ', '_')

    def get_task_config(self, name):
        return luigi.configuration.get_config().get(self.task_family, name)

    def log_slurm_info(self, command_output):
        matches = re.search('[0-9]+', command_output)
        if matches:
            jobid = matches.group(0)
            with open(self.auditlog_file, 'a') as alog:
                # Write jobid to audit log
                tsv_writer = csv.writer(alog, delimiter='\t')
                tsv_writer.writerow(['slurm_jobid', jobid])
                # Write slurm execution time to audit log
                (jobinfo_status, jobinfo_output) = self.execute_command('/usr/bin/sacct -j {jobid} --noheader --format=elapsed'.format(jobid=jobid))
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
                    tsv_writer.writerow(['slurm_exectime_sec', int(self.slurm_exectime_sec)])
