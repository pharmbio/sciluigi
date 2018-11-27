# Class to monitor and wait on AWS Batch Jobs

import boto3
import multiprocessing as mp
import time
import logging


class AWSBatchTaskWatcher():
    COMPLETED_JOB_STATES = {
        'SUCCEEDED',
        'FAILED',
        # A state I've added for jobs that no longer exist on batch.
        # This can be for older jobs whose status is deleted from AWS
        'DOESNOTEXIST'
    }
    POLLING_DELAY_SEC = 10
    JOB_WAIT_SECS = 1

    def pollJobState(self):
        while True:
            try:
                self.__log__.debug("Poll tick. {} jobs".format(
                    len(self.__jobStateDict__))
                )
                jobIDs_needing_update = [
                    jID for jID, state in self.__jobStateDict__.items()
                    if state not in self.COMPLETED_JOB_STATES
                ]
                if len(jobIDs_needing_update) > 0:
                    self.__log__.debug("Polling AWS about {} jobs".format(
                        len(jobIDs_needing_update))
                    )
                    update_result = self.__batch_client__.describe_jobs(
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
                    self.__jobStateDict__.update(updated_job_status)

                time.sleep(self.POLLING_DELAY_SEC)
            except BrokenPipeError:
                # Handle if the calling process ends, destroying the manager.
                # We should terminate too
                return

    def waitOnJob(self, jobID):
        # Works by adding this jobID to the dict if it does not exist
        if jobID not in self.__jobStateDict__:
            self.__log__.info("Adding jobId {} to our watch list".format(jobID))
            self.__jobStateDict__[jobID] = None
        # And then waiting for the polling child process to update the job status
        while self.__jobStateDict__[jobID] not in self.COMPLETED_JOB_STATES:
            self.__log__.debug("Still waiting on {}".format(jobID))
            time.sleep(self.JOB_WAIT_SECS)
        # Implicitly our job has reached a completed state
        self.__log__.info("JobID {} returned with status {}".format(
            jobID,
            self.__jobStateDict__[jobID]
        ))
        if self.__jobStateDict__[jobID] == 'DOESNOTEXIST':
            self.__log__.warning("JobID {} did not exist on batch".format(jobID))
        return self.__jobStateDict__[jobID]

    def __init__(
            self,
            session_options={},
            debug=False):
        # Logging first:
        self.__log__ = logging.getLogger('AWSBatchTaskWatcher')
        self.__log__.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        if debug:
            console_handler.setLevel(logging.DEBUG)
        else:
            console_handler.setLevel(logging.INFO)
        self.__log__.addHandler(console_handler)
        # BOTO3 / Batch client
        self.__session__ = boto3.Session(session_options)
        self.__batch_client__ = self.__session__.client(
            'batch'
        )
        # Use the multiprocessing manager to create a job state dict
        # that can safely be shared among processes
        self.__manager__ = mp.Manager()
        self.__jobStateDict__ = self.__manager__.dict()
        # Start a child process to poll batch for job status
        self.__jobStatePoller__ = mp.Process(target=self.pollJobState)
        self.__jobStatePoller__.start()

    def __del__(self):
        # Explicitly stop the polling process when this class is destroyed.
        self.__jobStatePoller__.terminate()
