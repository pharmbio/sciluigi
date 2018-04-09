import luigi
import sciluigi
import json
import logging
import subprocess
import docker
import os
from string import Template


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
    # Timeout in seconds
    timeout = None
    # Local Container cache location. For things like singularity that need to pull
    # And create a local container
    container_cache = None

    def __init__(self,
                 engine='docker',
                 vcpu=1,
                 mem=4096,
                 timeout=604800,  # Seven days of seconds
                 container_cache='.'):
        self.engine = engine
        self.vcpu = vcpu
        self.mem = mem
        self.timeout = timeout
        self.container_cache = container_cache

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
