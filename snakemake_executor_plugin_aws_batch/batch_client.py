import boto3

BATCH_SERVICE_NAME = "batch"


class BatchClient:
    def __init__(self, region_name=None):
        """
        Initialize an AWS client for a specific service using default credentials.

        :param region_name: The region name to use for the client (optional).
        """
        self.service_name = BATCH_SERVICE_NAME
        self.region_name = region_name
        self.client = self.initialize_batch_client()

    def initialize_batch_client(self):
        """
        Create an AWS Batch Client using boto3 with the default credentials.

        :return: The boto3 client for the specified service.
        """
        try:
            if self.region_name:
                return boto3.client(self.service_name, region_name=self.region_name)
            return boto3.client(self.service_name)

        except Exception as e:
            raise Exception(f"Failed to initialize {self.service_name} client: {e}")

    def register_job_definition(self, **kwargs):
        """
        Register a job definition in AWS Batch.

        :param kwargs: The keyword arguments to pass to register_job_definition method.
        :return: The response from the register_job_definition method.
        """
        return self.client.register_job_definition(**kwargs)

    def submit_job(self, **kwargs):
        """
        Submit a job to AWS Batch.

        :param kwargs: The keyword arguments to pass to the submit_job method.
        :return: The response from the submit_job method.
        """
        return self.client.submit_job(**kwargs)

    def describe_jobs(self, **kwargs):
        """
        Describe jobs in AWS Batch.

        :param kwargs: The keyword arguments to pass to the describe_jobs method.
        :return: The response from the describe_jobs method.
        """
        return self.client.describe_jobs(**kwargs)

    def deregister_job_definition(self, **kwargs):
        """
        Deregister a job definition in AWS Batch.

        :param kwargs: The keyword arguments passed to deregister_job_definition method.
        :return: The response from the deregister_job_definition method.
        """
        return self.client.deregister_job_definition(**kwargs)

    def terminate_job(self, **kwargs):
        """
        Terminate a job in AWS Batch.

        :param kwargs: The keyword arguments to pass to the terminate_job method.
        :return: The response from the terminate_job method.
        """
        return self.client.terminate_job(**kwargs)

    def describe_job_queues(self, **kwargs):
        """
        Describe job queues in AWS Batch.

        :param kwargs: The keyword arguments to pass to the describe_job_queues method.
        :return: The response from the describe_job_queues method.
        """
        return self.client.describe_job_queues(**kwargs)

    def describe_compute_environments(self, **kwargs):
        """
        Describe compute environments in AWS Batch.

        :param kwargs: The keyword arguments to pass to the describe_compute_environments method.
        :return: The response from the describe_compute_environments method.
        """
        return self.client.describe_compute_environments(**kwargs)
