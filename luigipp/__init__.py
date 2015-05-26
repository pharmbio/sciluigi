import luigi
import time
import random
import string

# ==============================================================================

class LuigiPPTask(luigi.Task):
    '''
    A Luigi task meta-class, implementing methods for supporting
    dynamic definition of upstream targets, in Luigi

    This has to be implemented as a meta-class rather than a mixin, since it
    overrides the requires() of luigi.Task method.
    '''
    def get_upstream_targets(self):
        upstream_tasks = []
        for param_val in self.param_args:
            if type(param_val) is dict:
                if 'upstream' in param_val:
                    upstream_tasks.append(param_val['upstream']['task'])
        return upstream_tasks

    def requires(self):
        return self.get_upstream_targets()

    def get_input(self, input_name):
        param = self.param_kwargs[input_name]
        if type(param) is dict and 'upstream' in param:
            return param['upstream']['task'].output()[param['upstream']['port']]
        else:
            return param

    def get_path(self, input_name):
        return self.get_input(input_name).path

    def get_value(self, input_name):
        param = self.param_kwargs[input_name]
        if type(param) is dict and 'upstream' in param:
            input_target = param['upstream']['task'].output()[param['upstream']['port']]
            if os.path.isfile(input_target.path):
                with input_target.open() as infile:
                    csv_reader = csv.reader(infile)
                    for row in csv_reader:
                        if row[0] == param['upstream']['key']:
                            return row[1]
            else:
                return 'NA'
        else:
            return param

    def new_target(self, basestr=None, **kwargs):
        if 'dep' in kwargs:
            path = self.get_path(kwargs['dep'])
            if 'ext' in kwargs:
                path += kwargs['ext']
        elif basestr is not None:
            path = basestr

        return luigi.LocalTarget(path)


    def outport(self, portname):
        return { 'upstream' : { 'task' : self, 'port' : portname } }

# ==============================================================================

class LuigiPPExternalTask(luigi.ExternalTask):
    '''
    The same as DependencyHelpers, for luigi.ExternalTask rather than
    luigi.Task
    '''
    def requires(self):
        upstream_tasks = []
        for param_val in self.param_args:
            if 'upstream' in param_val:
                upstream_tasks.append(param_val['upstream']['task'])
        return upstream_tasks

    def get_input(self, input_name):
        param = self.param_kwargs[input_name]
        if type(param) is dict and 'upstream' in param:
            return param['upstream']['task'].output()[param['upstream']['port']]
        else:
            return param

    def outport(self, portname):
        return { 'upstream' : { 'task' : self, 'port' : portname } }

# ==============================================================================

class HPCHelpers():
    '''
    Mixin with various convenience methods that most tasks need, such as for executing SLURM
    commands
    '''
    accounted_project = luigi.Parameter()

    # Main Execution methods
    def execute_in_configured_mode(self, command):
        '''Execute either locally or via SLURM, depending on config'''

        if self.get_task_config("runmode") == "local":
            self.execute_command(command)

        elif self.get_task_config("runmode") == "nodejob":
            train_size = "NA"
            if hasattr(self, 'train_size'):
                train_size = self.train_size
            replicate_id = "NA"
            if hasattr(self, 'replicate_id'):
                replicate_id = self.replicate_id

            self.execute_hpcjob(command,
                    accounted_project = self.accounted_project,
                    time_limit = self.get_task_config("time_limit"),
                    partition  = self.get_task_config("partition"),
                    cores      = self.get_task_config("cores"),
                    jobname    = "".join([train_size,
                                          replicate_id,
                                          self.dataset_name,
                                          self.task_family]),
                    threads    = self.get_task_config("threads"))

        elif self.get_task_config("runmode") == "mpijob":
            self.execute_mpijob(command,
                    accounted_project = self.accounted_project,
                    time_limit = self.get_task_config("time_limit"),
                    partition  = self.get_task_config("partition"),
                    cores      = self.get_task_config("cores"),
                    jobname    = "".join([self.train_size,
                                          self.replicate_id,
                                          self.dataset_name,
                                          self.task_family]))

    def execute_command(self, command):

        if isinstance(command, list):
            command = " ".join(command)

        log.info("Executing command: " + str(command))
        (status, output) = commands.getstatusoutput(command)
        log.info("STATUS: " + str(status))
        log.info("OUTPUT: " + "; ".join(str(output).split("\n")))
        if status != 0:
            log.error("Command failed: {cmd}".format(cmd=command))
            log.error("OUTPUT OF FAILED COMMAND: " + "; \n".join(str(output).split("\n")))
            raise Exception("Command failed: {cmd}\nOutput:\n{output}".format(cmd=command, output=output))
        return (status, output)

    def execute_hpcjob(self, command, accounted_project, time_limit="4:00:00", partition="node", cores=16, jobname="LuigiNodeJob", threads=16):

        slurm_part = "salloc -A {pr} -p {pt} -n {c} -t {t} -J {m} srun -n 1 -c {thr} ".format(
                pr  = accounted_project,
                pt  = partition,
                c   = cores,
                t   = time_limit,
                m   = jobname,
                thr = threads)

        if isinstance(command, list):
            command = " ".join(command)

        (status, output) = self.execute_command(slurm_part + command)
        self.log_slurm_info(output)

        return (status, output)

    def execute_mpijob(self, command, accounted_project, time_limit="4-00:00:00", partition="node", cores=32, jobname="LuigiMPIJob", cores_per_node=16):

        slurm_part = "salloc -A {pr} -p {pt} -n {c} -t {t} -J {m} mpirun -v -np {c} ".format(
                pr = accounted_project,
                pt = partition,
                c  = cores,
                t  = time_limit,
                m  = jobname)

        if isinstance(command, list):
            command = " ".join(command)

        (status, output) = self.execute_command(slurm_part + command)
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
        if not bool(re.match("^{c}+$".format(c=char_class), a_string)):
            raise Exception("String {s} does not match character class {cc}".format(s=a_string, cc=char_class))

    def clean_filename(self, filename):
        return re.sub("[^A-Za-z0-9\_\ ]", '_', str(filename)).replace(' ', '_')

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

# ==============================================================================

#class AuditTrailHelpers():
#    '''
#    Mixin for luigi.Task:s, with functionality for writing audit logs of running tasks
#    '''
#    start_time = None
#    end_time = None
#    exec_time = None
#
#    replicate_id = luigi.Parameter()
#
#    @luigi.Task.event_handler(luigi.Event.START)
#    def save_start_time(self):
#        task_name = self.task_id.split('(')[0]
#        unique_id = time.strftime('%Y%m%d.%H%M%S', time.localtime()) + '.' + ''.join(random.choice(string.ascii_lowercase) for _ in range(3))
#        base_dir = 'audit/{replicate_id}'.format(
#                    replicate_id=self.replicate_id
#                )
#        self.auditlog_file = '{base_dir}/{dataset_name}.{task_name}.{unique_id}.audit.log'.format(
#                    base_dir=base_dir,
#                    dataset_name=self.dataset_name,
#                    task_name=task_name,
#                    unique_id=unique_id
#                )
#        self.start_time = time.time()
#        if not os.path.exists(base_dir):
#            os.makedirs(base_dir)
#        with open(self.auditlog_file, 'w') as log:
#            tsv_writer = csv.writer(log, delimiter='\t')
#            # Write the value of all the tasks variables to the audit log
#            for k, v in self.__dict__.iteritems():
#                tsv_writer.writerow([k, v])
#
#    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
#    def save_end_time(self, task_exectime_sec):
#        self.end_time = time.time()
#        if hasattr(self, 'slurm_exectime_sec'):
#            self.slurm_queuetime_sec = int(task_exectime_sec) - int(self.slurm_exectime_sec)
#        if hasattr(self, 'auditlog_file'):
#            with open(self.auditlog_file, 'a') as log:
#                tsv_writer = csv.writer(log, delimiter='\t')
#                tsv_writer.writerow(['end_time', int(self.end_time)])
#                if hasattr(self, 'slurm_exectime_sec'):
#                    tsv_writer.writerow(['slurm_queuetime_sec', int(self.slurm_queuetime_sec)])
#                    tsv_writer.writerow(['total_tasktime_sec', int(task_exectime_sec)])
#                    tsv_writer.writerow(['derived_runtime_sec', int(self.slurm_exectime_sec)])
#                else:
#                    tsv_writer.writerow(['total_tasktime_sec', int(task_exectime_sec)])
#                    tsv_writer.writerow(['derived_runtime_sec', int(task_exectime_sec)])
#        else:
#            log.info("No audit_log set, so not writing audit log for " + str(self))

# ==============================================================================
