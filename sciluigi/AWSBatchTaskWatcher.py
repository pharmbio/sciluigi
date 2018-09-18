# Class to monitor and wait on AWS Batch Jobs

import boto3
import multiprocessing as mp
import time
import logging


class AWSBatchTaskWatcher():
    COMPLETED_JOB_STATES = {
        'SUCCEEDED',
        'FAILED',
        'DOESNOTEXIST'
    }
    POLLING_DELAY_SEC = 10
    JOB_WAIT_SECS = 1

    def pollJobState(self):
        while True:
            jobIDs_needing_update = [
                jID for jID, state in self.jobStateDict.items()
                if state not in self.COMPLETED_JOB_STATES
            ]
            if len(jobIDs_needing_update) > 0:
                update_result = self.batch_client.describe_jobs(
                    jobs=jobIDs_needing_update
                )
                update_result_jobs = update_result.get('jobs', [])
                updated_job_status = {
                    j['jobId']: j['status']
                    for j in update_result_jobs
                }
                jobIdsWithoutResult = list(set(jobIDs_needing_update) - set(updated_job_status.keys()))
                updated_job_status.update({
                    jID: "DOESNOTEXIST"
                    for jID in jobIdsWithoutResult
                })
                self.jobStateDict.update(updated_job_status)

            time.sleep(self.POLLING_DELAY_SEC)

    def waitOnJob(self, jobID):
        # Works by adding this jobID to the dict if it does not exist
        if jobID not in self.jobStateDict:
            self.log.info("Adding jobId {} to our list".format(jobID))
            self.jobStateDict[jobID] = None
        # And then waiting for the polling child process to update the job status
        while self.jobStateDict[jobID] not in self.COMPLETED_JOB_STATES:
            time.sleep(self.JOB_WAIT_SECS)
        # Implicitly our job has reached a completed state
        if self.jobStateDict[jobID] == 'DOESNOTEXIST':
            self.log.warning("JobID {} did not exist on batch".format(jobID))

    def __init__(
            self,
            session_options={}):
        # Logging first:
        self.log = logging.getLogger('AWSBatchTaskWatcher')
        self.log.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        self.log.addHandler(console_handler)
        # BOTO3 / Batch client
        self.session = boto3.session(session_options)
        self.batch_client = self.session.client(
            'batch'
        )
        # Use the multiprocessing manager to create a job state dict
        # that can safely be shared among processes
        self.manager = mp.Manager()
        self.jobStateDict = self.manager.dict()
        # Start a child process to poll batch for job status
        self.jobStatePoller = mp.Process(target=self.pollJobState)

    def __del__(self):
        # Explicitly stop the polling process when this class is destroyed.
        self.jobStatePoller.terminate()
