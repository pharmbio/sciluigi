import luigi
import time

# ================================================================================

class SlurmInfo(object):
    project = None
    partition = None
    cores = None
    time = None
    jobname = None
    threads = None

    def __init__(self, project, partition, cores, time, jobname, threads):
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
        #FIXME: Double-check the format of this one!
        argstr = ' -A {pr} -p {pt} -n {c} -t {t} -J {j} '.format(
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
        argstr = ' -A {pr} -p {pt} -n {c} -t {t} -J {j} srun -n 1 -c {thr} '.format(
                pr = self.project,
                pt = self.partition,
                c = self.cores,
                t = self.time,
                j = self.jobname,
                thr = self.threads)
        return argstr


# ================================================================================


class SlurmHelpers():
    '''
    Mixin with various convenience methods for executing jobs via SLURM
    '''
    # Luigi parameters
    accounted_project = luigi.Parameter()

    # Other class-fields
    slurminfo = None # Class: SlurmInfo
    runmode = None # One of RUNMODE_LOCAL|RUNMODE_HPC|RUNMODE_MPI

    # A few 'constants'
    RUNMODE_LOCAL = 'runmode_local'
    RUNMODE_HPC = 'runmode_hpc'
    RUNMODE_MPI = 'runmode_mpi'

    # Main Execution methods
    def ex(self, command):
        '''
        Execute either locally or via SLURM, depending on config
        '''
        if self.runmode == self.RUNMODE_LOCAL:
            self.ex_local(command)
        elif self.runmode == self.RUNMODE_HPC:
            self.ex_hpc(command)
        elif self.runmode == self.RUNMODE_MPI:
            self.ex_mpi(command)


    def ex_local(self, command):
        # If list, convert to string
        if isinstance(command, list):
            command = ' '.join(command)

        log.info('Executing command: ' + str(command))
        (status, output) = commands.getstatusoutput(command) # TODO: Replace with subprocess call!

        # Take care of errors
        if status != 0:
            msg = 'Command failed: {cmd}\nOutput:\n{output}'.format(cmd=command, output=output)
            log.error(msg)
            raise Exception(msg)

        return (status, output)


    def ex_hpc(self, command):
        if isinstance(command, list):
            command = ' '.join(command)

        (status, output) = self.ex_local(slurm_part + command)

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
            with open(self.auditlog_file, 'a') as alog:
                # Write jobid to audit log
                tsv_writer = csv.writer(alog, delimiter='\t')
                tsv_writer.writerow(['slurm_jobid', jobid])
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
                    tsv_writer.writerow(['slurm_exectime_sec', int(self.slurm_exectime_sec)])
