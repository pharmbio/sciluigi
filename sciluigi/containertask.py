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
import time
import io
from botocore.exceptions import ClientError

try:
    from urlparse import urlsplit, urljoin
except ImportError:
    from urllib.parse import urlsplit, urljoin

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
    # Format is {'source_path': {'bind': '/container/path', 'mode': mode}}
    mounts = None
    # Local Container cache location. For things like singularity that need to pull
    # And create a local container
    container_cache = None

    # AWS specific stuff
    aws_jobRoleArn = None
    aws_s3_scratch_loc = None
    aws_batch_job_queue = None
    aws_secrets_loc = None
    aws_boto_max_tries = None

    # SLURM specifics
    slurm_partition = None

    def __init__(self,
                 engine='docker',
                 vcpu=1,
                 mem=4096,
                 timeout=10080,  # Seven days of minutes
                 mounts={},
                 container_cache='.',
                 aws_jobRoleArn='',
                 aws_s3_scratch_loc='',
                 aws_batch_job_queue='',
                 aws_secrets_loc=os.path.expanduser('~/.aws'),
                 aws_boto_max_tries=10,
                 slurm_partition=None,
                 ):
        self.engine = engine
        self.vcpu = vcpu
        self.mem = mem
        self.timeout = timeout
        self.mounts = mounts
        self.container_cache = container_cache

        self.aws_jobRoleArn = aws_jobRoleArn
        self.aws_s3_scratch_loc = aws_s3_scratch_loc
        self.aws_batch_job_queue = aws_batch_job_queue
        self.aws_secrets_loc = aws_secrets_loc
        self.aws_boto_max_tries = aws_boto_max_tries

        self.slurm_partition = slurm_partition

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

    def map_targets_to_container(self, targets):
        """
        Accepts a dictionary where the keys are identifiers for various targets
        and the value is the target

        This breaks down the targets by their schema (file, s3, etc).
        For each schema a lowest-common-path is found and a suggested container 
        mountpoint is generated

        What one gets back is a nested dict
        {
            'scheme': {
                'common_prefix': '/path/on/source/shared/by/all/targets/of/schema',
                'rel_paths': {
                    'identifier': 'path_rel_to_common_prefix'
                }
                'targets': {
                    'identifier': target,
                }
            }
        }
        """
        # Determine the schema for these targets via comprehension
        schema = {t.scheme for t in targets.values()}
        return_dict = {}
        for scheme in schema:
            return_dict[scheme] = {}
            # Get only the targets for this scheme
            scheme_targets = {i: t for i, t in targets.items() if t.scheme == scheme}
            common_prefix = os.path.commonprefix(
                [os.path.dirname(
                    os.path.join(
                        urlsplit(t.path).netloc,
                        urlsplit(t.path).path
                        )
                ) for t in scheme_targets.values()])
            return_dict[scheme]['common_prefix'] = common_prefix
            return_dict[scheme]['targets'] = scheme_targets
            return_dict[scheme]['relpaths'] = {
                                                    i: os.path.relpath(
                                                            os.path.join(
                                                                urlsplit(t.path).netloc,
                                                                urlsplit(t.path).path
                                                            ),
                                                            common_prefix)
                                                    for i, t in scheme_targets.items()
                                                }
        return return_dict

    def mounts_CP_DF_UF(
            self,
            input_targets,
            output_targets,
            inputs_mode,
            outputs_mode,
            input_mount_point,
            output_mount_point):

        container_paths = {}
        mounts = self.containerinfo.mounts.copy()
        UF = []
        DF = []

        output_target_maps = self.map_targets_to_container(
            output_targets,
        )
        out_schema = set(output_target_maps.keys())
        # Local file targets can just be mapped.
        file_output_common_prefix = None
        if 'file' in out_schema:
            file_output_common_prefix = output_target_maps['file']['common_prefix']
            mounts[os.path.abspath(output_target_maps['file']['common_prefix'])] = {
                'bind': os.path.join(output_mount_point, 'file'),
                'mode': outputs_mode
            }
            container_paths.update({
                i: os.path.join(output_mount_point, 'file', rp)
                for i, rp in output_target_maps['file']['relpaths'].items()
            })
            out_schema.remove('file')
        # Handle other schema here using BCW, creating the appropriate UF parameters
        for scheme in out_schema:
            for identifier in output_target_maps[scheme]['targets']:
                container_paths[identifier] = os.path.join(
                    output_mount_point,
                    scheme,
                    output_target_maps[scheme]['relpaths'][identifier]
                    )
                UF.append("{}::{}".format(
                    container_paths[identifier],
                    output_target_maps[scheme]['targets'][identifier].path
                ))

        input_target_maps = self.map_targets_to_container(
            input_targets
        )
        in_schema = set(input_target_maps.keys())
        if 'file' in in_schema:
            # Check for the edge case where our common prefix for input and output is the same
            if file_output_common_prefix and file_output_common_prefix == input_target_maps['file']['common_prefix']:
                # It is! Skip adding a mount for inputs then, and reset our input mountpoint
                input_mount_point = output_mount_point
                pass
            else:  # Add our mount
                mounts[os.path.abspath(input_target_maps['file']['common_prefix'])] = {
                    'bind': os.path.join(input_mount_point, 'file'),
                    'mode': inputs_mode
                }
            container_paths.update({
                i: os.path.join(input_mount_point, 'file', rp)
                for i, rp in input_target_maps['file']['relpaths'].items()
            })
            in_schema.remove('file')

        # Handle other schema here using BCW, creating the appropriate DF parameters
        for scheme in in_schema:
            for identifier in input_target_maps[scheme]['targets']:
                container_paths[identifier] = os.path.join(
                    input_mount_point,
                    scheme,
                    input_target_maps[scheme]['relpaths'][identifier]
                    )
                DF.append("{}::{}::{}".format(
                    input_target_maps[scheme]['targets'][identifier].path,
                    container_paths[identifier],
                    inputs_mode,
                ))

        # Mount the AWS secrets if we have some AND s3 is in one of our schema
        if self.containerinfo.aws_secrets_loc and ('s3' in out_schema or 's3' in in_schema):
            mounts[self.containerinfo.aws_secrets_loc] = {'bind': '/root/.aws', 'mode': 'ro'}
        
        return (mounts, container_paths, DF, UF)

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
            input_targets={},
            output_targets={},
            extra_params={},
            inputs_mode='ro',
            outputs_mode='rw',
            input_mount_point='/mnt/inputs',
            output_mount_point='/mnt/outputs'):
        if self.containerinfo.engine == 'docker':
            return self.ex_docker(
                command,
                input_targets,
                output_targets,
                extra_params,
                inputs_mode,
                outputs_mode,
                input_mount_point,
                output_mount_point
            )
        elif self.containerinfo.engine == 'aws_batch':
            return self.ex_aws_batch(
                command,
                input_targets,
                output_targets,
                extra_params,
                inputs_mode,
                outputs_mode,
                input_mount_point,
                output_mount_point
            )
        elif self.containerinfo.engine == 'singularity_slurm':
            return self.ex_singularity_slurm(
                command,
                input_targets,
                output_targets,
                extra_params,
                inputs_mode,
                outputs_mode,
                input_mount_point,
                output_mount_point
            )
        else:
            raise Exception("Container engine {} is invalid".format(self.containerinfo.engine))

    def ex_singularity_slurm(
            self,
            command,
            input_targets={},
            output_targets={},
            extra_params={},
            inputs_mode='ro',
            outputs_mode='rw',
            input_mount_point='/mnt/inputs',
            output_mount_point='/mnt/outputs'):
        """
        Run command in the container using singularity on slurm, with mountpoints
        command is assumed to be in python template substitution format
        """
        mounts, container_paths, DF, UF = self.mounts_CP_DF_UF(
            input_targets,
            output_targets,
            inputs_mode,
            outputs_mode,
            input_mount_point,
            output_mount_point)

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
                    "docker://{}".format(self.container)
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            print(pull_proc)
            # Move back
            os.chdir(cwd)

        template_dict = container_paths.copy()
        template_dict.update(extra_params)
        command = Template(command).substitute(template_dict)

        log.info("Attempting to run {} in {}".format(
                command,
                self.container
            ))

        command_list = [
            'singularity', 'exec', '--contain', '--scratch', '/scratch'
        ]
        for mp in mounts:
            command_list += ['-B', "{}:{}:{}".format(mp, mounts[mp]['bind'], mounts[mp]['mode'])]
        command_list.append(img_location)
        command_list += ['bucket_command_wrapper', '-c', command]
        for uf in UF:
            command_list += ['-UF', uf]
        for df in DF:
            command_list += ['-DF', df]

        if not self.containerinfo.slurm_partition:  # No slurm partition. Run without slurm
            command_proc = subprocess.run(
                command_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        else:
            command_proc = subprocess.run(
                [
                    'salloc',
                    '-c', str(self.containerinfo.vcpu),
                    '--mem={}M'.format(self.containerinfo.mem),
                    '-t', str(self.containerinfo.timeout),
                    '-p', self.containerinfo.slurm_partition,
                ]+command_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

        log.info(command_proc.stdout)
        if command_proc.stderr:
            log.warn(command_proc.stderr)

    def ex_aws_batch(
            self,
            command,
            input_targets={},
            output_targets={},
            extra_params={},
            inputs_mode='ro',
            outputs_mode='rw',
            input_mount_point='/mnt/inputs',
            output_mount_point='/mnt/outputs'):
        """
        Run a command in a container using AWS batch.
        Handles uploading of files to / from s3 and then into the container.
        Assumes the container has batch_command_wrapper.py
        """
        #
        # The steps:
        #   1) Upload local input files to S3 scratch bucket/key
        #   2) Register / retrieve the job definition
        #   3) submit the job definition with parameters filled with this specific command
        #   4) Retrieve the output paths from the s3 scratch bucket / key
        #

        # Only import AWS libs as needed
        import boto3
        batch_client = boto3.client('batch')
        s3_client = boto3.client('s3')

        run_uuid = str(uuid.uuid4())

        # We need mappings for both to and from S3 and from S3 to within the container
        # <local fs> <-> <s3> <-> <Container Mounts>
        # The script in the container, bucket_command_wrapper.py, handles the second half
        # practically, but we need to provide the link s3://bucket/key::/container/path/file::mode
        # the first half we have to do here.

        container_paths = {}  # Dict key is command template key. Value is in-container path
        UF = set()  # Set of UF lines to be added. Format is container_path::bucket_file_uri
        DF = set()  # Set of UF lines to be added. Format is bucket_file_uri::container_path::mode
        needs_s3_download = set()  # Set of  Tuples. (s3::/bucket/key, target)
        s3_temp_to_be_deleted = set()  # S3 paths to be deleted.

        # Group our output targets by schema
        output_target_maps = self.map_targets_to_container(
            output_targets,
        )
        out_schema = set(output_target_maps.keys())
        # Make our container paths
        for schema, schema_targets in output_target_maps.items():
            for k, relpath in schema_targets['relpaths'].items():
                container_paths[k] = os.path.join(
                    output_mount_point,
                    schema,
                    relpath
                )     
        # Inputs too
        # Group by schema
        input_target_maps = self.map_targets_to_container(
            input_targets,
        )
        in_schema = set(input_target_maps.keys())
        # Make our container paths
        for schema, schema_targets in input_target_maps.items():
            for k, relpath in schema_targets['relpaths'].items():
                container_paths[k] = os.path.join(
                    input_mount_point,
                    schema,
                    relpath
                )
        # Container paths should be done now.

        # Now the need to handle our mapping to-from S3.
        # Inputs
        for scheme, schema_targets in input_target_maps.items():
            if scheme == 's3':  # Already coming from S3. Just make our DF entry
                for k, target in schema_targets['targets'].items():
                    DF.add('{}::{}::{}'.format(
                        target.path,
                        container_paths[k],
                        inputs_mode
                    ))
            else:  # NOT in S3. Will need to be upload to a temp location
                for k, target in schema_targets['targets'].items():
                    s3_temp_loc = os.path.join(
                            self.containerinfo.aws_s3_scratch_loc,
                            run_uuid,
                            scheme,
                            'in',
                            schema_targets['relpaths'][k]
                        )
                    # Add to DF for inside the container
                    DF.add('{}::{}::{}'.format(
                        s3_temp_loc,
                        container_paths[k],
                        inputs_mode
                    ))
                    # If we are read-write, we can add this to our todo list later
                    if inputs_mode == 'rw':
                        needs_s3_download.add((
                            s3_temp_loc,
                            target
                        ))
                    # And actually upload to the S3 temp location now
                    if scheme == 'file' or scheme == '':
                        s3_client.upload_file(
                            Filename=os.path.abspath(target.path),
                            Bucket=urlsplit(s3_temp_loc).netloc,
                            Key=urlsplit(s3_temp_loc).path.strip('/'),
                            ExtraArgs={
                                'ServerSideEncryption': 'AES256'
                            }
                        )
                    else:
                        # Have to use BytesIO because luigi targets can ONLY be opened in 
                        # binary mode, and upload / download fileobj can ONLY accept binary mode files
                        # For reasons.
                        s3_client.upload_fileobj(
                            Fileobj=io.BytesIO(
                                target.open('r').read().encode('utf-8')
                            ),
                            Bucket=urlsplit(s3_temp_loc).netloc,
                            Key=urlsplit(s3_temp_loc).path.strip('/'),
                            ExtraArgs={
                                'ServerSideEncryption': 'AES256'
                            }
                        )
                    s3_temp_to_be_deleted.add(s3_temp_loc)

        # Outputs
        for scheme, schema_targets in output_target_maps.items():
            if scheme == 's3':  # Already going to S3. Just make our UF entry
                for k, target in schema_targets['targets'].items():
                    UF.add('{}::{}'.format(
                        container_paths[k],
                        target.path,
                    ))
            else:  # NOT ending in S3. Will need to download to target and make a temp destination in s3
                for k, target in schema_targets['targets'].items():
                    s3_temp_loc = os.path.join(
                            self.containerinfo.aws_s3_scratch_loc,
                            run_uuid,
                            scheme,
                            'out',
                            schema_targets['relpaths'][k]
                        )
                    # Add to UF for inside the container
                    UF.add('{}::{}'.format(
                        container_paths[k],
                        s3_temp_loc
                    ))
                    # add this to our download from s3 list later
                    needs_s3_download.add((
                        s3_temp_loc,
                        target
                    ))
                    s3_temp_to_be_deleted.add(s3_temp_loc)

        # 2) Register / retrieve job definition for this container, command, and job role arn

        # Make a UUID based on the container / command
        job_def_name = "sl_containertask__{}".format(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    self.container+self.containerinfo.aws_jobRoleArn+str(self.containerinfo.mounts)
                    )
            )

        # Search to see if this job is ALREADY defined.
        boto_tries = 0
        while boto_tries < self.containerinfo.aws_boto_max_tries:
            boto_tries += 1
            try:
                job_def_search = batch_client.describe_job_definitions(
                    maxResults=1,
                    status='ACTIVE',
                    jobDefinitionName=job_def_name,
                )
                break
            except ClientError:
                log.info("Caught boto3 client error, sleeping for 10 seconds")
                time.sleep(10)
        if len(job_def_search['jobDefinitions']) == 0:
            # Not registered yet. Register it now
            log.info(
                """Registering job definition for {} with role {} and mounts {} under name {}
                """.format(
                           self.container,
                           self.containerinfo.aws_jobRoleArn,
                           self.containerinfo.mounts,
                           job_def_name,
                ))
            # To be passed along for container properties
            aws_volumes = []
            aws_mountPoints = []
            for (host_path, container_details) in self.containerinfo.mounts.items():
                name = str(uuid.uuid5(uuid.NAMESPACE_URL, host_path))
                aws_volumes.append({
                    'host': {'sourcePath': host_path},
                    'name': name
                })
                if container_details['mode'].lower() == 'ro':
                    read_only = True
                else:
                    read_only = False
                aws_mountPoints.append({
                    'containerPath': container_details['bind'],
                    'sourceVolume': name,
                    'readOnly': read_only,
                })

            boto_tries = 0
            while boto_tries < self.containerinfo.aws_boto_max_tries:
                boto_tries += 1
                try:
                    batch_client.register_job_definition(
                        jobDefinitionName=job_def_name,
                        type='container',
                        containerProperties={
                            'image': self.container,
                            'vcpus': 1,
                            'memory': 1024,
                            'command': shlex.split(command),
                            'jobRoleArn': self.containerinfo.aws_jobRoleArn,
                            'mountPoints': aws_mountPoints,
                            'volumes': aws_volumes
                        },
                        timeout={
                            'attemptDurationSeconds': self.containerinfo.timeout * 60
                        }
                    )
                    break
                except ClientError:
                    log.info("Caught boto3 client error, sleeping for 10 seconds")
                    time.sleep(10)
        else:  # Already registered
            aws_job_def = job_def_search['jobDefinitions'][0]
            log.info('Found job definition for {} with job role {} under name {}'.format(
                aws_job_def['containerProperties']['image'],
                aws_job_def['containerProperties']['jobRoleArn'],
                job_def_name,
            ))

        # Build our container command list
        template_dict = container_paths.copy()
        template_dict.update(extra_params)
        container_command_list = [
            'bucket_command_wrapper',
            '--command', Template(command).safe_substitute(template_dict)
        ]
        # Add in our inputs
        for df in DF:
            container_command_list += [
                '-DF',
                df
            ]

        # And our outputs
        for uf in UF:
            container_command_list += [
                '-UF',
                uf
            ]

        # Submit the job
        boto_tries = 0
        while boto_tries < self.containerinfo.aws_boto_max_tries:
            boto_tries += 1
            try:
                job_submission = batch_client.submit_job(
                    jobName=run_uuid,
                    jobQueue=self.containerinfo.aws_batch_job_queue,
                    jobDefinition=job_def_name,
                    containerOverrides={
                        'vcpus': self.containerinfo.vcpu,
                        'memory': self.containerinfo.mem,
                        'command': container_command_list,
                    },
                )
                break
            except ClientError:
                log.info("Caught boto3 client error, sleeping for 10 seconds")
                time.sleep(10)
        job_submission_id = job_submission.get('jobId')
        log.info("Running {} under jobId {}".format(
            container_command_list,
            job_submission_id
        ))
        while True:
            try:
                job_status = batch_client.describe_jobs(
                    jobs=[job_submission_id]
                ).get('jobs')[0]
            except ClientError:
                log.info("Caught boto3 client error, sleeping for 10 seconds")
            if job_status.get('status') == 'SUCCEEDED' or job_status.get('status') == 'FAILED':
                break
            time.sleep(10)
        if job_status.get('status') != 'SUCCEEDED':
            raise Exception("Batch job failed. {}".format(
                job_status.get('statusReason')
            ))
        # Implicit else we succeeded
        # Now we need to copy back from S3 to our local filesystem
        for (s3_loc, target) in needs_s3_download:
            if target.scheme == 'file':
                s3_client.download_file(
                    Bucket=urlsplit(s3_loc).netloc,
                    Key=urlsplit(s3_loc).path.strip('/'),
                    Filename=os.path.abspath(target.path),
                )
            else:
                with target.open('w') as target_h:
                    s3_client.download_file(
                        Bucket=urlsplit(s3_loc).netloc,
                        Key=urlsplit(s3_loc).path.strip('/'),
                        Fileobj=target_h,
                    )
        # Cleanup the temp S3
        for s3_path in s3_temp_to_be_deleted:
            s3_client.delete_object(
                Bucket=urlsplit(s3_path).netloc,
                Key=urlsplit(s3_path).path.strip('/'),
            )

        # And done

    def ex_docker(
            self,
            command,
            input_targets={},
            output_targets={},
            extra_params={},
            inputs_mode='ro',
            outputs_mode='rw',
            input_mount_point='/mnt/inputs',
            output_mount_point='/mnt/outputs'):
        """
        Run command in the container using docker, with mountpoints
        command is assumed to be in python template substitution format
        """
        client = docker.from_env()

        mounts, container_paths, DF, UF = self.mounts_CP_DF_UF(
            input_targets,
            output_targets,
            inputs_mode,
            outputs_mode,
            input_mount_point,
            output_mount_point)

        template_dict = container_paths.copy()
        template_dict.update(extra_params)
        command = Template(command).substitute(template_dict)

        command_list = [
            'bucket_command_wrapper',
            '--command', command,
            ]
        for df in DF:
            command_list.append('-DF')
            command_list.append(df)
        for uf in UF:
            command_list.append('-UF')
            command_list.append(uf)

        try:
            log.info("Attempting to run {} in {} with mounts {}".format(
                command_list,
                self.container,
                mounts,
            ))
            stdout = client.containers.run(
                image=self.container,
                command=command_list,
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
