#!/usr/bin/env python3

# This is an example of how the ContainerTask and ContainerTargetInfo
# classes extend sciluigi, and allow commands to be run in containers
# seamlessly on different container engines / HPC systems
# Start, ironically, at the bottom and work your way up!

import logging
import luigi
import sciluigi as sl
import argparse
import os
from subprocess import call

log = logging.getLogger('sciluigi-interface')

# ------------------------------------------------------------------------
# Workflow class(es)
# ------------------------------------------------------------------------


class MyWorkflow(sl.WorkflowTask):
    # Here are some parameters to define how we want to run our container
    engine = sl.Parameter()
    aws_secrets_loc = sl.Parameter()
    # Only when using AWS_batch
    jobRoleArn = sl.Parameter(default="")
    s3_scratch_loc = sl.Parameter(default="")
    batch_job_queue = sl.Parameter(default="")

    def workflow(self):
        rawdata = self.new_task('rawdata', RawData)

        # Run first without a container
        atot = self.new_task(
            'atot',
            AToT)
        atot.in_data = rawdata.out_rawdata

        # And now in a container!
        # To run in a container, we have to
        # specify which engine, and parameters we need
        # This is done through a ContainerInfo class
        # We will initialize via the parameters we recieved
        # from the command line
        test_containerinfo = sl.ContainerInfo(
            engine=self.engine,
            vcpu=1,  # Number of vCPU to request
            mem=256,  # Memory in MB
            timeout=5,  # time in minutes
            aws_secrets_loc=self.aws_secrets_loc,
            aws_jobRoleArn=self.jobRoleArn,
            aws_s3_scratch_loc=self.s3_scratch_loc,
            aws_batch_job_queue=self.batch_job_queue,
        )
        # Now actually start the task.
        # Note: This allows different instances of the same task
        # to use different engines, queues, etc as needed
        atot_in_container = self.new_task(
            'atot_in_container',
            AToT_ContainerTask,
            containerinfo=test_containerinfo,
        )
        atot_in_container.in_data = rawdata.out_rawdata
        return (atot, atot_in_container)

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------


class RawData(sl.ExternalTask):
    # It's perfectly fine to combine local, external and container tasks
    # all in one workflow.
    def out_rawdata(self):
        return sl.ContainerTargetInfo(self, 'data/acgt.txt')


class AToT(sl.Task):
    # Here is the non-containerized version of this task
    in_data = None

    def out_replatot(self):
        return sl.TargetInfo(self, self.in_data().path + '.atot')

    # ------------------------------------------------

    def run(self):
        cmd = 'cat ' + self.in_data().path + ' | sed "s/A/T/g" > ' + self.out_replatot().path
        log.info("COMMAND TO EXECUTE: " + cmd)
        call(cmd, shell=True)


class AToT_ContainerTask(sl.ContainerTask):
    # Here is the containerized version of this task.
    # In this simple example, there isn't much advantage of running in a container
    # But when dealing with specialized software (requiring complex and brittle dependencies)
    # or with heavy tasks needing big harware to run, there is an advantage.
    # This task will run identically locally via docker, on AWS, or via PBS.

    # ALL ContainerTasks must specify which container is to be used
    container = 'golob/sciluigi-example:0.1.0__bcw.0.3.0'

    # Dependencies (inputs) are the same as in a non-containerized task
    in_data = None

    def out_replatot(self):
        # ContainerTargetInfo will take care of shifting files to and from
        # cloud providers (S3 at this time) and your local filesystems
        return sl.ContainerTargetInfo(self, self.in_data().path + '.container.atot')

    # ------------------------------------------------

    def run(self):
        # ContainerTasks use the python string template system to handle inputs and outputs
        # Same command as above, but with template placeholders $inFile for in and $outFile.
        # This often works out neater than the more complex string combinations as above
        # in the non-containerized task. 
        cmd = 'cat $inFile | sed "s/A/T/g" > $outFile'
        self.ex(
            command=cmd,
            input_targets={  # A dictionary with the key being the template placeholder
                'inFile': self.in_data(),  # Value is a ContainerTarget
            },
            output_targets={
                'outFile': self.out_replatot()  # Same drill for outputs
            },
            extra_params={}  # Optional dict of other placeholders to fill in the command string
        )


# Run this file as script
# ------------------------------------------------------------------------

if __name__ == '__main__':
    # Depending on the container engine used, you must specify some basic settings
    # This can be done (as here) via CLI settings, a config file, or even hard-wired
    # into scripts.

    parser = argparse.ArgumentParser(description="""
    Containertask example for sciluigi""")
    subparsers = parser.add_subparsers(
        help='Which container engine to use',
        dest='engine',
        required=True,
    )
    # If we are going to shuffle to-from AWS-S3 we need to know secrets
    parser.add_argument(
        '--aws-secrets-loc',
        help="""Where are the AWS secrets located""",
        default=os.path.expanduser('~/.aws'),
        metavar='~/.aws'
    )
    # The simplest case is docker; all one needs for docker is to have it installed.
    docker_parser = subparsers.add_parser('docker')

    # AWS-batch has a few options that must be specified to work
    # Including which account, queue, and a directory to store temporary scripts.
    aws_parser = subparsers.add_parser("aws_batch")
    aws_parser.add_argument(
        '--jobRoleArn',
        help="""Job role to use when submitting to batch""",
        required=True,
        metavar='arn:aws:iam::12345:role/somerole'
    )
    aws_parser.add_argument(
        '--s3-scratch-loc',
        help="""Temporary S3 location to transiently keep input/output files.
        format: s3://bucket/key/prefix/""",
        required=True,
        metavar='s3://bucket/key/prefix/to/temp/loc/'
    )
    aws_parser.add_argument(
        '--batch-job-queue',
        help="""To which batch queue should the jobs be submitted?""",
        required=True,
        metavar='some_queue_name'
    )
    # PBS has a few options that must be specified to work
    # Including which account, queue, and distinct from AWS,
    # a directory to store temporary scripts AND singularity containers;
    # these directories must be on a shared file system visible to nodes
    pbs_parser = subparsers.add_parser("pbs")
    pbs_parser.add_argument(
        '--container_cache', '-cc',
        help="""Location to store temporary singularity containers for pbs / slurm.
        Must be on a shared file system to work properly.""",
        required=True,
    )
    pbs_parser.add_argument(
        '--account',
        help="""Account to use for PBS job submission""",
        required=True,
    )
    pbs_parser.add_argument(
        '--queue',
        help="""Into which PBS queue should the jobs be submitted.""",
        required=True,
    )
    pbs_parser.add_argument(
        '--scriptpath',
        help="""Location on a shared file system to store temporary scripts""",
        required=True,
    )

    args = parser.parse_args()
    # Extract these parameters to the arguments for our workflow
    args_list = [
        "--{}={}".format(k.replace('_', '-'), v)
        for k, v in vars(args).items()
    ]

    sl.run(
        local_scheduler=True,
        main_task_cls=MyWorkflow,
        cmdline_args=args_list,
    )
