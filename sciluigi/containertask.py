import luigi
import sciluigi
import json
import logging
import subprocess
import docker
import os
from string import Template
import shlex
import uuid

# Setup logging
log = logging.getLogger('sciluigi-interface')


class ContainerInfo():
    """
    A data object to store parameters related to running a specific
    tasks in a container (docker / batch / etc). Mostly around resources.
    """
    # Which container system to use
    # Docker by default. Extensible in the future for batch, slurm-singularity, etc
    engine = None
    # num vcpu required
    vcpu = None
    # max memory (mb)
    mem = None
    # Env
    env = None
    # Timeout in minutes
    timeout = None
    # Local Container cache location. For things like singularity that need to pull
    # And create a local container
    container_cache = None

    # AWS specific stuff
    aws_jobRoleArn = None
    aws_s3_scratch_loc = None

    def __init__(self,
                 engine='docker',
                 vcpu=1,
                 mem=4096,
                 timeout=604800,  # Seven days of seconds
                 container_cache='.',
                 aws_jobRoleArn='',
                 aws_s3_scratch_loc='',
                 ):
        self.engine = engine
        self.vcpu = vcpu
        self.mem = mem
        self.timeout = timeout
        self.container_cache = container_cache
        self.aws_jobRoleArn = aws_jobRoleArn
        self.aws_s3_scratch_loc = aws_s3_scratch_loc

    def __str__(self):
        """
        Return string of this information
        """
        return(
            "{} with Cpu {}, Mem {} MB, timeout {} secs, and container cache {}".format(
                self.engine,
                self.vcpu,
                self.mem,
                self.timeout,
                self.container_cache
            ))


class ContainerInfoParameter(sciluigi.parameter.Parameter):
    '''
    A specialized luigi parameter, taking ContainerInfo objects.
    '''

    def parse(self, x):
        if isinstance(x, ContainerInfo):
            return x
        else:
            log.error('parameter is not instance of ContainerInfo. It is instead {}'
                      .format(type(x)))
            raise Exception('parameter is not instance of ContainerInfo. It is instead {}'
                            .format(type(x)))


class ContainerHelpers():
    """
    Mixin with various methods and variables for running commands in containers using (Sci)-Luigi
    """
    # Other class-fields
    # Resource guidance for this container at runtime.
    containerinfo = ContainerInfoParameter(default=None)

    # The ID of the container (docker registry style).
    container = None

    def map_paths_to_container(self, paths, container_base_path='/mnt'):
        """
        Accepts a dictionary where the keys are identifiers for various targets
        and the value is the HOST path for that target

        What this does is find a common HOST prefix
        and remaps to the CONTAINER BASE PATH

        Returns a dict of the paths for the targets as they would be seen
        if the common prefix is mounted within the container at the container_base_path
        """
        common_prefix = os.path.commonprefix(
            [os.path.dirname(p) for p in paths.values()]
        )
        container_paths = {
            i: os.path.join(
                container_base_path,
                os.path.relpath(paths[i], common_prefix))
            for i in paths
        }
        return os.path.abspath(common_prefix), container_paths

    def make_fs_name(self, uri):
        uri_list = uri.split('://')
        if len(uri_list) == 1:
            name = uri_list[0]
        else:
            name = uri_list[1]
        keepcharacters = ('.', '_')
        return "".join(c if (c.isalnum() or c in keepcharacters) else '_' for c in name).rstrip()

    def ex(
            self,
            command,
            input_paths={},
            output_paths={},
            mounts={},
            inputs_mode='ro',
            outputs_mode='rw'):
        if self.containerinfo.engine == 'docker':
            return self.ex_docker(
                command,
                input_paths,
                output_paths,
                mounts,
                inputs_mode,
                outputs_mode
            )
        elif self.containerinfo.engine == 'aws_batch':
            return self.ex_aws_batch(
                command,
                input_paths,
                output_paths,
                mounts,
                inputs_mode,
                outputs_mode
            )
        elif self.containerinfo.engine == 'singularity_slurm':
            return self.ex_singularity_slurm(
                command,
                input_paths,
                output_paths,
                mounts,
                inputs_mode,
                outputs_mode
            )
        else:
            raise Exception("Container engine {} is invalid".format(self.containerinfo.engine))

    def ex_singularity_slurm(
            self,
            command,
            input_paths={},
            output_paths={},
            mounts={},
            inputs_mode='ro',
            outputs_mode='rw'):
        """
        Run command in the container using singularity, with mountpoints
        command is assumed to be in python template substitution format
        """
        container_paths = {}

        if len(output_paths) > 0:
            output_host_path_ca, output_container_paths = self.map_paths_to_container(
                output_paths,
                container_base_path='/mnt/outputs'
            )
            container_paths.update(output_container_paths)
            mounts[output_host_path_ca] = {'bind': '/mnt/outputs', 'mode': outputs_mode}

        if len(input_paths) > 0:
            input_host_path_ca, input_container_paths = self.map_paths_to_container(
                input_paths,
                container_base_path='/mnt/inputs'
            )
            # Handle the edge case where the common directory for inputs is equal to the outputs
            if len(output_paths) > 0 and (output_host_path_ca == input_host_path_ca):
                log.warn("Input and Output host paths the same {}".format(output_host_path_ca))
                # Repeat our mapping, now using the outputs path for both
                input_host_path_ca, input_container_paths = self.map_paths_to_container(
                    input_paths,
                    container_base_path='/mnt/outputs'
                )
            else:  # output and input paths different OR there are only input paths
                mounts[input_host_path_ca] = {'bind': '/mnt/inputs', 'mode': inputs_mode}

            # No matter what, add our mappings
            container_paths.update(input_container_paths)

        img_location = os.path.join(
            self.containerinfo.container_cache,
            "{}.singularity.img".format(self.make_fs_name(self.container))
            )
        log.info("Looking for singularity image {}".format(img_location))
        if not os.path.exists(img_location):
            log.info("No image at {} Creating....".format(img_location))
            try:
                os.makedirs(os.path.dirname(img_location))
            except FileExistsError:
                # No big deal
                pass
            # Singularity is dumb and can only pull images to the working dir
            # So, get our current working dir. 
            cwd = os.getcwd()
            # Move to our target dir
            os.chdir(os.path.dirname(img_location))
            # Attempt to pull our image
            pull_proc = subprocess.run(
                [
                    'singularity',
                    'pull',
                    '--name',
                    os.path.basename(img_location),
                    self.container
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            print(pull_proc)
            # Move back
            os.chdir(cwd)

        command = Template(command).substitute(container_paths)
        log.info("Attempting to run {} in {}".format(
                command,
                self.container
            ))

        command_list = [
            'singularity', 'exec'
        ]
        for mp in mounts:
            command_list += ['-B', "{}:{}:{}".format(mp, mounts[mp]['bind'], mounts[mp]['mode'])]
        command_list.append(img_location)
        command_list += shlex.split(command)
        command_proc = subprocess.run(
            command_list,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        log.info(command_proc.stdout)
        if command_proc.stderr:
            log.warn(command_proc.stderr)

    def ex_aws_batch(
            self,
            command,
            input_paths={},
            output_paths={},
            mounts={},
            inputs_mode='ro',
            outputs_mode='rw'):
        """
        Run a command in a container using AWS batch.
        Handles uploading of files to / from s3 and then into the container. 
        Assumes the container has batch_command_wrapper.py
        """
        #
        # The steps:
        #   1) Register / retrieve the job definition
        #   2) Upload local input files to S3 scratch bucket/key
        #   3) submit the job definition with parameters filled with this specific command
        #   4) Retrieve the output paths from the s3 scratch bucket / key
        #

        # Only import AWS libs as needed
        import boto3
        batch_client = boto3.client('batch')
        s3_client = boto3.client('s3')

        # First a bit of file mapping / uploading of input items
        for (key, path) in input_paths.items():

            print(key, "::", path)

        # 1) Register / retrieve job definition

        # Make a UUID based on the container / command
        job_def_name = "sl_containertask__{}".format(
                uuid.uuid5(uuid.NAMESPACE_URL, self.container+command)
            )

        # Search to see if this job is ALREADY defined.
        job_def_search = batch_client.describe_job_definitions(
            maxResults=1,
            jobDefinitionName=job_def_name,
        )
        if len(job_def_search['jobDefinitions']) == 0:
            # Not registered yet. Register it now
            log.info('Registering job definition for {} in {} under name {}'.format(
                command,
                self.container,
                job_def_name,
            ))
            batch_client.register_job_definition(
                jobDefinitionName=job_def_name,
                type='container',
                containerProperties={
                    'image': self.container,
                    'vcpus': 123,
                    'memory': 123,
                    'command': shlex.split(command),
                    'jobRoleArn': self.containerinfo.aws_jobRoleArn,
                },
                timeout={
                    'attemptDurationSeconds': self.containerinfo.timeout * 60 
                }
            )
        else:  # Already registered
            aws_job_def = job_def_search['jobDefinitions'][0]
            log.info('Found job definition for {} in {} under name {}'.format(
                aws_job_def['containerProperties']['command'],
                aws_job_def['containerProperties']['image'],
                job_def_name,
            ))




    def ex_docker(
            self,
            command,
            input_paths={},
            output_paths={},
            mounts={},
            inputs_mode='ro',
            outputs_mode='rw'):
        """
        Run command in the container using docker, with mountpoints
        command is assumed to be in python template substitution format
        """
        client = docker.from_env()
        container_paths = {}

        if len(output_paths) > 0:
            output_host_path_ca, output_container_paths = self.map_paths_to_container(
                output_paths,
                container_base_path='/mnt/outputs'
            )
            container_paths.update(output_container_paths)
            mounts[output_host_path_ca] = {'bind': '/mnt/outputs', 'mode': outputs_mode}

        if len(input_paths) > 0:
            input_host_path_ca, input_container_paths = self.map_paths_to_container(
                input_paths,
                container_base_path='/mnt/inputs'
            )
            # Handle the edge case where the common directory for inputs is equal to the outputs
            if len(output_paths) > 0 and (output_host_path_ca == input_host_path_ca):
                log.warn("Input and Output host paths the same {}".format(output_host_path_ca))
                # Repeat our mapping, now using the outputs path for both
                input_host_path_ca, input_container_paths = self.map_paths_to_container(
                    input_paths,
                    container_base_path='/mnt/outputs'
                )
            else:  # output and input paths different OR there are only input paths
                mounts[input_host_path_ca] = {'bind': '/mnt/inputs', 'mode': inputs_mode}

            # No matter what, add our mappings
            container_paths.update(input_container_paths)

        command = Template(command).substitute(container_paths)

        try:
            log.info("Attempting to run {} in {}".format(
                command,
                self.container
            ))
            stdout = client.containers.run(
                image=self.container,
                command=command,
                volumes=mounts,
                mem_limit="{}m".format(self.containerinfo.mem),
            )
            log.info(stdout)
            return (0, stdout, "")
        except docker.errors.ContainerError as e:
            log.error("Non-zero return code from the container: {}".format(e))
            return (-1, "", "")
        except docker.errors.ImageNotFound:
            log.error("Could not find container {}".format(
                self.container)
                )
            return (-1, "", "")
        except docker.errors.APIError as e:
            log.error("Docker Server failed {}".format(e))
            return (-1, "", "")
        except Exception as e:
            log.error("Unknown error occurred: {}".format(e))
            return (-1, "", "")


# ================================================================================

class ContainerTask(ContainerHelpers, sciluigi.task.Task):
    '''
    luigi task that includes the ContainerHelpers mixin.
    '''
    pass
