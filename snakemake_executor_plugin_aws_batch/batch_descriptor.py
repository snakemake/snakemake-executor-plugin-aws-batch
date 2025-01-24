import threading
import heapq
import time


class BatchJobDescriber:
    """
    This singleton class handles calling the AWS Batch DescribeJobs API with up to 100
    job IDs per request, then dispensing each job description to the thread interested
    in it. This helps avoid AWS API request rate limits when tracking concurrent jobs.
    """

    JOBS_PER_REQUEST = 100  # maximum jobs per DescribeJob request

    def __init__(self):
        self.lock = threading.Lock()
        self.last_request_time = 0
        self.job_queue = []
        self.jobs = {}

    def describe(self, aws, job_id, period):
        """get the latest Batch job description"""
        while True:
            with self.lock:
                if job_id not in self.jobs:
                    # register new job to be described ASAP
                    heapq.heappush(self.job_queue, (0.0, job_id))
                    self.jobs[job_id] = None
                # update as many job descriptions as possible
                self._update(aws, period)
                # return the desired job description if we have it
                desc = self.jobs[job_id]
                if desc:
                    return desc
            # otherwise wait (outside the lock) and try again
            time.sleep(period / 4)

    def unsubscribe(self, job_id):
        """unsubscribe from job_id when no longer interested"""
        with self.lock:
            if job_id in self.jobs:
                del self.jobs[job_id]

    def _update(self, aws, period):
        # if enough time has passed since our last DescribeJobs request
        if time.time() - self.last_request_time >= period:
            # take the N least-recently described jobs
            job_ids = set()
            assert self.job_queue
            while self.job_queue and len(job_ids) < self.JOBS_PER_REQUEST:
                job_id = heapq.heappop(self.job_queue)[1]
                assert job_id not in job_ids
                if job_id in self.jobs:
                    job_ids.add(job_id)
            if not job_ids:
                return
            # describe them
            try:
                job_descs = aws.describe_jobs(jobs=list(job_ids))
            finally:
                # always: bump last_request_time and re-enqueue these jobs
                self.last_request_time = time.time()
                for job_id in job_ids:
                    heapq.heappush(self.job_queue, (self.last_request_time, job_id))
            # update self.jobs with the new descriptions
            for job_desc in job_descs["jobs"]:
                job_ids.remove(job_desc["jobId"])
                self.jobs[job_desc["jobId"]] = job_desc
            assert (
                not job_ids
            ), "AWS Batch DescribeJobs didn't return all expected results"